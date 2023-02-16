import asyncio
import json
import time
from asyncio import Queue, Task, create_task
from typing import Optional

import requests
from db.models import Category
from logger_config import parser_logger as logger
from settings import POSTGRES_URL
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select

from .constants import (BASE_URL, LAST_PAGE_TRESHOLD, MAX_BRANDS_IN_REQUEST,
                        MAX_ITEMS_IN_BRANDS_FILTER, MAX_ITEMS_IN_REQUEST,
                        MAX_PAGE, MIN_PRICE_RANGE, QUERY_PARAMS)
from .schemas import (BrandSchema, ColorSchema, HistorySizeRelationSchema,
                      ItemSchema, ItemsHistorySchema, SizeSchema)


async def get_items_ids(item: dict, queue: Queue) -> None:

    shard: str = item.get('shard')

    if shard != 'blackhole':
        start: float = time.time()
        item_id: int = item.get('id')
        query: str = item.get('query')
        price_filter_url: str = (f'{BASE_URL}{shard}/v4/'
                                 f'filters?{query}{QUERY_PARAMS}')
        try:
            response: dict = requests.get(price_filter_url).json()
            ctg_filters: list[dict] = response.get('data').get('filters')
            for ctg_filter in ctg_filters:
                if ctg_filter.get('key') == 'priceU':
                    ctg_max_price: int = ctg_filter.get('maxPriceU')
                    break

            ids_list: list[int] = basic_parsing(
                item_id, shard, query, 0, ctg_max_price
            )
        except json.decoder.JSONDecodeError:
            logger.debug('a JSONDecode error occured at: %s', price_filter_url)

        finish: float = time.time()
        impl_time: float = round(finish - start, 2)
        logger.info('parsed %s %s in %d seconds', shard, query, impl_time)

    await queue.put((item_id, ids_list))


def basic_parsing(item_id: int,
                  shard: str,
                  query: str,
                  min_pr: int,
                  max_pr: int) -> list[int]:
    logger.debug('basic parsing for %s %s, price range: %s;%s',
                 shard, query, min_pr, max_pr)

    result: list[int] = []

    price_lmt: str = f'&priceU={min_pr};{max_pr}'

    base_url: str = (f'{BASE_URL}{shard}/catalog?'
                     f'{QUERY_PARAMS}&{query}{price_lmt}')

    def check_last_page_is_full() -> bool:
        last_page_url: str = base_url + '&page=' + str(MAX_PAGE)
        try:
            response: dict = requests.get(last_page_url).json()
            response_data: list[dict] = response.get('data').get('products')

            return len(response_data) > LAST_PAGE_TRESHOLD

        except json.decoder.JSONDecodeError:
            logger.debug('a JSONDecode error occured at: %s', last_page_url)
            check_last_page_is_full()

    if check_last_page_is_full():
        rnd_avg: int = round((max_pr + min_pr) // 2 + 100, -4)
        if rnd_avg - min_pr >= MIN_PRICE_RANGE:
            result.extend(
                basic_parsing(item_id, shard, query, min_pr, rnd_avg)
            )
            result.extend(
                basic_parsing(item_id, shard, query, rnd_avg, max_pr)
            )
        else:
            result.extend(parse_by_brand(item_id, shard, query, price_lmt))

    else:
        ids_list: list[int] = parse_through_pages(item_id, base_url)
        result.extend(ids_list)

    return result


def parse_by_brand(item_id: int,
                   shard: str,
                   query: str,
                   price_lmt: str) -> list[int]:

    start: float = time.time()
    logger.debug(
        'parsing by brand for section %s, price range %s', item_id, price_lmt)

    base_url: str = (f'{BASE_URL}{shard}/catalog?'
                     f'{query}{QUERY_PARAMS}{price_lmt}')

    brand_filter_url: str = (f'{BASE_URL}{shard}/v4/filters?filters='
                             f'fbrand&{query}{QUERY_PARAMS}{price_lmt}')
    response: dict = requests.get(brand_filter_url).json()
    brand_filters: list[dict] = response.get(
        'data').get('filters')[0].get('items')

    concatenated_ids_list: list[str] = []
    concatenated_ids: str = ''
    cnt: int = 1

    for brand in brand_filters:
        brand_id: int = brand.get('id')
        brand_count: int = brand.get('count')
        if brand_count > MAX_ITEMS_IN_BRANDS_FILTER:
            concatenated_ids_list.append(str(brand_id))
        elif cnt < MAX_BRANDS_IN_REQUEST:
            concatenated_ids = ';'.join([concatenated_ids, str(brand_id)])
            cnt += 1
        else:
            concatenated_ids_list.append(concatenated_ids[1:])
            concatenated_ids = str(brand_id)
            cnt = 1

    concatenated_ids_list.append(concatenated_ids[1:])

    number_of_requests: int = len(concatenated_ids_list)
    result: list[int] = []

    for idx, string in enumerate(concatenated_ids_list, 1):
        request_url: str = base_url + '&fbrand=' + string
        ids_list: list[int] = parse_through_pages(item_id, request_url)
        result.extend(ids_list)

        logger.debug('%d / %d requests done', idx, number_of_requests)

    finish: float = time.time()
    impl_time: float = round(finish - start, 2)
    logger.info('parsing by brand for section %s, price range %s '
                'done in %d seconds', item_id, price_lmt, impl_time)

    return result


def parse_through_pages(item_id: int, base_url: str) -> list[int]:
    result: list[int] = []
    for page in range(1, MAX_PAGE + 1):

        def scrape_page(page: int) -> Optional[bool]:
            url: str = base_url + '&page=' + str(page)

            try:
                response: dict = requests.get(url).json()
                response_data: list[dict] = response.get(
                    'data').get('products')

                if not len(response_data):
                    return True

                for item in response_data:
                    result.append(item.get('id'))

            except json.decoder.JSONDecodeError:
                logger.debug('a JSONDecode error occured at: %s', url)
                scrape_page(page)

        continue_iteration: Optional[bool] = scrape_page(page)
        if continue_iteration:
            break

    return result


async def concatenate_ids(queue1: Queue, queue2: Queue) -> None:
    while True:
        goods: tuple[int, list[int]] = await queue1.get()

        logger.debug(f'concatenating ids for {goods[0]}')

        concatenated_ids: str = ''
        cnt: int = 0

        for item_id in goods[1]:
            if cnt < MAX_ITEMS_IN_REQUEST:
                concatenated_ids = ';'.join([concatenated_ids, str(item_id)])
                cnt += 1
            else:
                await queue2.put((goods[0], concatenated_ids[1:]))
                concatenated_ids = str(item_id)
                cnt = 0

        await queue2.put((goods[0], concatenated_ids[1:]))
        queue1.task_done()

        logger.debug(f'finished concatenating ids for {goods[0]}')


async def get_cards(queue2: Queue, queue3: Queue) -> None:
    while True:
        items: tuple[int, str] = await queue2.get()

        logger.debug(f'getting cards for {items[0]}')

        base_url: str = (f'https://card.wb.ru/cards/detail?'
                         f'spp=30{QUERY_PARAMS}&nm=')

        url: str = base_url + items[1]
        response: requests.Response = requests.get(url)

        cards: list[dict] = response.json().get('data').get('products')

        await queue3.put((items[0], cards))
        queue2.task_done()

        logger.debug(f'finished getting cards for {items[0]}')


async def collect_data(queue: Queue) -> None:
    while True:
        start: float = time.time()
        items: tuple[int, list[dict]] = await queue.get()
        logger.debug('collecting data for %d', items[0])

        item_objects: list[dict] = []
        size_objects: list[dict] = []
        brand_objects: list[dict] = []
        item_history_objects: list[dict] = []
        color_objects: list[dict] = []

        for item in items[1]:
            colors: list[dict] = item.get('colors')
            for color in colors:
                color_object = ColorSchema(**color)
                color_objects.append(color_object.dict())
            item['color'] = 999999 if len(color) > 1 else color.get('id')

            sum_count: int = 0
            hash_sizes: dict = {}
            for size in item.get('sizes'):
                size_count: int = 0
                for stock in size.get('stocks'):
                    item_count: int = stock.get('qty')
                    if item_count:
                        size_count += item_count
                hash_sizes[size.get('name')] = size_count
                sum_count += size_count

                size_object = SizeSchema(**size)
                size_objects.append(size_object.dict())

            item['category'] = items[0]
            item['item'] = item.get('id')
            item['timestamp'] = time.time()
            item['sum_count'] = sum_count

            item_object = ItemSchema(**item)
            brand_object = BrandSchema(**item)
            item_history_object = ItemsHistorySchema(**item)
            # history_size_object = HistorySizeRelationSchema()

            item_objects.append(item_object.dict())
            brand_objects.append(brand_object.dict())
            item_history_objects.append(item_history_object.dict())

        queue.task_done()

        finish: float = time.time()
        impl_time: float = finish - start
        logger.info('collected data for %d: %s items in %d seconds',
                    items[0], len(item_objects), impl_time)

        from pprint import pprint
        print('======= ITEM =======')
        pprint(item_objects[0])
        print('======= SIZE =======')
        pprint(size_objects[0])
        print('======= BRAND =======')
        pprint(brand_objects[0])
        print('======= ITEM_HISTORY =======')
        pprint(item_history_objects[0])
        print('======= COLOR =======')
        pprint(color_objects[1])


async def parsing_manager(categories: list[dict]) -> None:

    queue1: Queue = Queue()
    queue2: Queue = Queue()
    queue3: Queue = Queue()

    get_items_ids_tasks: list[Task] = [create_task(
        get_items_ids(item, queue1)) for item in categories]

    infinite_tasks: list[Task] = []
    infinite_tasks.append(create_task(concatenate_ids(queue1, queue2)))
    infinite_tasks.append(create_task(get_cards(queue2, queue3)))
    infinite_tasks.append(create_task(collect_data(queue3)))

    await asyncio.gather(*get_items_ids_tasks)

    await queue1.join()
    await queue2.join()
    await queue3.join()

    for task in infinite_tasks:
        task.cancel()


def load_all_items() -> None:
    engine = create_engine(POSTGRES_URL)

    with Session(engine) as session:
        selectable: Select = select(Category).where(Category.id.in_([128456]))
        # selectable: Select = select(Category)
        categories: list[dict] = [
            _.__dict__ for _ in session.scalars(selectable)
        ]

    asyncio.run(parsing_manager(categories))
