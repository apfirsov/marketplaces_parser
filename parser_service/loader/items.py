import asyncio
import json
import sys
import time
from asyncio import Queue, Task, create_task
from typing import Generator

import requests
from db.models import Category
from logger_config import parser_logger as logger
from settings import POSTGRES_URL
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select

from .constants import (BASE_URL, LAST_PAGE_TRESHOLD, MAX_BRANDS_IN_REQUEST,
                        MAX_ITEMS_IN_BRANDS_FILTER, MAX_ITEMS_IN_REQUEST,
                        MAX_PAGE, MAX_REQUEST_RETRIES, MIN_PRICE_RANGE,
                        QUERY_PARAMS)
from .schemas import (BrandSchema, ColorSchema, HistorySizeRelationSchema,
                      ItemSchema, ItemsHistorySchema, SizeSchema)

# все запросы асинхронные!!


class CategoriesStack:

    def __init__(self) -> None:
        self.items: list = []

    def put(self, item) -> None:
        self.items.append(item)

    def __iter__(self) -> Generator:
        for item in self.items:
            yield item
        # yield None


class ItemsParser:

    def __init__(self) -> None:
        self.queue1: Queue = Queue()
        self.queue2: Queue = Queue()
        self.queue3: Queue = Queue()

    async def get_items_ids(self, item: dict) -> None:

        # if item is None:
        #     await queue.put(None)

        shard: str = item.get('shard')

        if shard != 'blackhole':
            start: float = time.time()
            item_id: int = item.get('id')
            query: str = item.get('query')
            price_filter_url: str = (f'{BASE_URL}{shard}/v4/'
                                     f'filters?{query}{QUERY_PARAMS}')

            error_counter: int = MAX_REQUEST_RETRIES
            while error_counter:
                try:
                    response: dict = requests.get(price_filter_url).json()
                    ctg_filters: list[dict] = response.get(
                        'data').get('filters')
                    for ctg_filter in ctg_filters:
                        if ctg_filter.get('key') == 'priceU':
                            ctg_max_price: int = ctg_filter.get('maxPriceU')
                            break

                    ids_list: list[int] = self._basic_parsing(
                        item_id, shard, query, 0, ctg_max_price
                    )
                    break

                except json.decoder.JSONDecodeError:
                    error_counter -= 1
                    if not error_counter:
                        logger.critical(
                            'exceeded JSONDecode error limit at: %s',
                            price_filter_url
                        )
                        sys.exit()
                    logger.debug(
                        'JSONDecode error occured at: %s, %d tries left',
                        price_filter_url, error_counter
                    )

            finish: float = time.time()
            impl_time: float = round(finish - start, 2)
            logger.info('parsed %s %s in %d seconds', shard, query, impl_time)

        await self.queue1.put((item_id, ids_list))

    def _basic_parsing(self, item_id: int,
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

        last_page_url: str = base_url + '&page=' + str(MAX_PAGE)
        error_counter: int = MAX_REQUEST_RETRIES
        while error_counter:
            try:
                response: dict = requests.get(last_page_url).json()
                resp_data: list[dict] = response.get('data').get('products')

                last_page_is_full: bool = len(resp_data) > LAST_PAGE_TRESHOLD
                break

            except json.decoder.JSONDecodeError:
                error_counter -= 1
                if not error_counter:
                    logger.critical('exceeded JSONDecode error limit at: %s',
                                    last_page_url)
                    sys.exit()
                logger.debug('JSONDecode error occured at: %s, %d tries left',
                             last_page_url, error_counter)

        if last_page_is_full:
            rnd_avg: int = round((max_pr + min_pr) // 2 + 100, -4)
            if rnd_avg - min_pr >= MIN_PRICE_RANGE:
                result.extend(
                    self._basic_parsing(item_id, shard, query, min_pr, rnd_avg)
                )
                result.extend(
                    self._basic_parsing(item_id, shard, query, rnd_avg, max_pr)
                )
            else:
                result.extend(self._parse_by_brand(
                    item_id, shard, query, price_lmt)
                )

        else:
            ids_list: list[int] = self._parse_through_pages(base_url)
            result.extend(ids_list)

        return result

    def _parse_by_brand(self, item_id: int,
                        shard: str,
                        query: str,
                        price_lmt: str) -> list[int]:

        start: float = time.time()
        logger.debug(
            'parsing by brand for %s, price range %s', item_id, price_lmt)

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
            ids_list: list[int] = self._parse_through_pages(request_url)
            result.extend(ids_list)

            logger.debug('%d / %d requests done', idx, number_of_requests)

        finish: float = time.time()
        impl_time: float = round(finish - start, 2)
        logger.info('parsing by brand for section %s, price range %s '
                    'done in %d seconds', item_id, price_lmt, impl_time)

        return result

    def _parse_through_pages(self, base_url: str) -> list[int]:
        # сразу складывать в очередь по одному итему: (category.id, item.id)

        ids_list: list[int] = []

        page: int = 1
        error_counter: int = MAX_REQUEST_RETRIES

        while page <= MAX_PAGE:

            url: str = base_url + '&page=' + str(page)

            try:
                response: dict = requests.get(url).json()
                response_data: list[dict] = response.get(
                    'data').get('products')

                if not len(response_data):
                    break

                for item in response_data:
                    ids_list.append(item.get('id'))

                error_counter = 0
                page += 1

            except json.decoder.JSONDecodeError:
                error_counter -= 1
                if not error_counter:
                    logger.critical(
                        'exceeded JSONDecode error limit at: %s', url)
                    sys.exit()
                logger.debug('JSONDecode error occured at: %s, %d tries left',
                             url, error_counter)

        return ids_list

    # вар 1: если прилетел None, то break
    # вар 2: примитив ???
    # await event.wait()
    # break
    async def concatenate_ids(self) -> None:
        while True:  # объединить с get_cards чтобы айди объединялись и тут же летел запрос
            goods: tuple[int, list[int]] = await self.queue1.get()

            logger.debug(f'concatenating ids for {goods[0]}')

            concatenated_ids: str = ''
            cnt: int = 0

            for item_id in goods[1]:
                if cnt < MAX_ITEMS_IN_REQUEST:
                    concatenated_ids = ';'.join(
                        [concatenated_ids, str(item_id)])
                    cnt += 1
                else:
                    await self.queue2.put((goods[0], concatenated_ids[1:]))
                    concatenated_ids = str(item_id)
                    cnt = 0

            await self.queue2.put((goods[0], concatenated_ids[1:]))
            self.queue1.task_done()

            logger.debug(f'finished concatenating ids for {goods[0]}')

    async def get_cards(self) -> None:
        while True:
            items: tuple[int, str] = await self.queue2.get()

            logger.debug(f'getting cards for {items[0]}')

            base_url: str = (f'https://card.wb.ru/cards/detail?'
                             f'spp=30{QUERY_PARAMS}&nm=')

            url: str = base_url + items[1]
            response: requests.Response = requests.get(url)

            cards: list[dict] = response.json().get('data').get('products')

            await self.queue3.put((items[0], cards))
            self.queue2.task_done()

            logger.debug(f'finished getting cards for {items[0]}')

    async def collect_data(self) -> None:
        while True:
            start: float = time.time()
            items: tuple[int, list[dict]] = await self.queue3.get()
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

            self.queue3.task_done()

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


async def parsing_manager(categories: CategoriesStack) -> None:

    parser = ItemsParser()

    # create_task = worker, тут их около 2000, а ВБ скорее всего даст только 100
    # надо реализовать партионную обработку
    get_items_ids_tasks: list[Task] = [
        create_task(parser.get_items_ids(item)) for item in categories]

    infinite_tasks: list[Task] = []
    infinite_tasks.append(create_task(parser.concatenate_ids()))
    infinite_tasks.append(create_task(parser.get_cards()))
    infinite_tasks.append(create_task(parser.collect_data()))

    await asyncio.gather(*get_items_ids_tasks)

    await parser.queue1.join()
    await parser.queue2.join()
    await parser.queue3.join()

    for task in infinite_tasks:
        task.cancel()


def load_all_items() -> None:
    engine = create_engine(POSTGRES_URL)

    with Session(engine) as session:
        selectable: Select = select(Category).where(Category.id.in_([128456]))
        # selectable: Select = select(Category)
        categories = CategoriesStack()
        for category in session.scalars(selectable):
            categories.put(category.__dict__)

    asyncio.run(parsing_manager(categories))
