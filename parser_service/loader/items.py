import asyncio
import json
import sys
import time
from asyncio import Event, Queue, Semaphore, Task, create_task

import aiohttp
import pydantic
from db.models import Category
from logger_config import parser_logger as logger
from settings import POSTGRES_URL
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select

from .constants import (BASE_URL, LAST_PAGE_TRESHOLD, MAX_BRANDS_IN_REQUEST,
                        MAX_ITEMS_IN_BRANDS_FILTER, MAX_ITEMS_IN_REQUEST,
                        MAX_PAGE, MAX_REQUEST_RETRIES, MIN_PRICE_RANGE,
                        QUERY_PARAMS, SEMAPHORE_LIMIT)
from .schemas import ArticleSchema, ColorSchema


class ItemsParser:

    def __init__(self) -> None:
        self.timestamp: float = time.time()
        self.ids_queue: Queue = Queue()
        self.cards_queue: Queue = Queue()
        self.db_queue: Queue = Queue()

    async def get_items_ids(self, category: dict, semaphore) -> None:

        async with semaphore:
            start: float = time.time()
            category_id: int = category.get('id')
            shard: str = category.get('shard')
            query: str = category.get('query')
            price_filter_url: str = (f'{BASE_URL}{shard}/v4/'
                                     f'filters?{query}{QUERY_PARAMS}')

            error_counter: int = MAX_REQUEST_RETRIES
            while error_counter:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(price_filter_url) as r:
                            response: dict = await r.json(content_type=None)

                    ctg_filters: list[dict] = (
                        response.get('data').get('filters'))
                    for ctg_filter in ctg_filters:
                        if ctg_filter.get('key') == 'priceU':
                            ctg_max_price: int = ctg_filter.get('maxPriceU')
                            break
                    break

                except json.decoder.JSONDecodeError:
                    error_counter -= 1
                    if not error_counter:
                        logger.critical(
                            'exceeded JSONDecode error limit at: %s',
                            price_filter_url
                        )
                        sys.exit()
                    logger.info(
                        'JSONDecode error occured at: %s, %d tries left',
                        price_filter_url, error_counter
                    )

            await self._basic_parsing(
                category_id, shard, query, 0, ctg_max_price)

            finish: float = time.time()
            impl_time: float = round(finish - start, 2)
            logger.info('parsed %s %s in %d seconds',
                        shard, query, impl_time)

    async def _basic_parsing(self, category_id: int,
                             shard: str,
                             query: str,
                             min_pr: int,
                             max_pr: int) -> None:
        logger.info('basic parsing for %s %s, price range: %s;%s',
                    shard, query, min_pr, max_pr)

        price_lmt: str = f'&priceU={min_pr};{max_pr}'

        base_url: str = (f'{BASE_URL}{shard}/catalog?'
                         f'{QUERY_PARAMS}&{query}{price_lmt}')

        last_page_url: str = base_url + '&page=' + str(MAX_PAGE)
        error_counter: int = MAX_REQUEST_RETRIES

        while error_counter:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(last_page_url) as r:
                        response: dict = await r.json(content_type=None)
                    response_data: list[dict] = (
                        response.get('data').get('products'))

                    last_page_is_full: bool = (
                        len(response_data) > LAST_PAGE_TRESHOLD)
                    break

                except json.decoder.JSONDecodeError:
                    error_counter -= 1
                    if not error_counter:
                        logger.critical(
                            'exceeded JSONDecode error limit at: %s',
                            last_page_url
                        )
                        sys.exit()
                    logger.info('JSONDecode error occured at: %s, %d tries '
                                'left', last_page_url, error_counter)

        if last_page_is_full:
            rnd_avg: int = round((max_pr + min_pr) // 2 + 100, -4)
            if rnd_avg - min_pr >= MIN_PRICE_RANGE:
                await self._basic_parsing(
                    category_id, shard, query, min_pr, rnd_avg)
                await self._basic_parsing(
                    category_id, shard, query, rnd_avg, max_pr)
            else:
                await self._parse_by_brand(
                    category_id, shard, query, price_lmt)
        else:
            await self._get_items_ids_chunk(category_id, base_url)

    async def _parse_by_brand(self, category_id: int,
                              shard: str,
                              query: str,
                              price_lmt: str) -> list[int]:

        start: float = time.time()
        logger.info(
            'parsing by brand for %s, price range %s', category_id, price_lmt)

        base_url: str = (f'{BASE_URL}{shard}/catalog?'
                         f'{query}{QUERY_PARAMS}{price_lmt}')

        brand_filter_url: str = (f'{BASE_URL}{shard}/v4/filters?filters='
                                 f'fbrand&{query}{QUERY_PARAMS}{price_lmt}')

        async with aiohttp.ClientSession() as session:
            async with session.get(brand_filter_url) as r:
                response: dict = await r.json(content_type=None)
        brand_filters: list[dict] = (
            response.get('data').get('filters')[0].get('items'))

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

        for idx, string in enumerate(concatenated_ids_list, 1):
            request_url: str = base_url + '&fbrand=' + string
            await self._get_items_ids_chunk(category_id, request_url)

            logger.info('brand parsing for %s, %s: %d / %d requests done',
                        category_id, price_lmt, idx, number_of_requests)

        finish: float = time.time()
        impl_time: float = round(finish - start, 2)
        logger.info('parsing by brand for section %s, price range %s '
                    'done in %d seconds', category_id, price_lmt, impl_time)

    async def _get_items_ids_chunk(self,
                                   category_id: int,
                                   base_url: str) -> None:

        logger.info('getting items ids chunks')

        async with aiohttp.ClientSession() as session:

            page: int = 1
            error_counter: int = MAX_REQUEST_RETRIES
            concatenated_ids: str = ''
            cnt: int = 0

            while page <= MAX_PAGE:
                url: str = base_url + '&page=' + str(page)
                try:
                    async with session.get(url) as response:
                        response: dict = await response.json(content_type=None)
                        response_data: list[dict] = (
                            response.get('data').get('products'))

                    if not len(response_data):
                        await self.ids_queue.put(
                            (category_id, concatenated_ids[1:]))
                        await asyncio.sleep(3)
                        break

                    for item in response_data:
                        item_id: int = item.get('id')

                        if cnt < MAX_ITEMS_IN_REQUEST:
                            concatenated_ids = ';'.join(
                                [concatenated_ids, str(item_id)])
                            cnt += 1
                        else:
                            await self.ids_queue.put(
                                (category_id, concatenated_ids[1:]))
                            concatenated_ids = str(item_id)
                            cnt = 0

                    error_counter = 0
                    page += 1
                except json.decoder.JSONDecodeError:
                    error_counter -= 1
                    if not error_counter:
                        logger.critical(
                            'exceeded JSONDecode error limit at: %s', url)
                        sys.exit()
                    logger.debug('JSONDecode error at: %s, %d tries left',
                                 url, error_counter)

    async def get_cards(self) -> None:
        while True:
            category_id: int
            concatenated_ids: str
            category_id, concatenated_ids = await self.ids_queue.get()

            base_url: str = (f'https://card.wb.ru/cards/detail?'
                             f'spp=30{QUERY_PARAMS}&nm=')

            async with aiohttp.ClientSession() as session:
                url: str = base_url + concatenated_ids

                async with session.get(url) as response:
                    response: dict = await response.json(content_type=None)
                    response_data: list[dict] = (
                        response.get('data').get('products'))

            await self.cards_queue.put((category_id, response_data))
            self.ids_queue.task_done()

            logger.info(f'got cards chunk for {category_id}')

    async def collect_data(self) -> None:
        while True:
            start: float = time.time()
            category_id: int
            cards: list[dict]
            category_id, cards = await self.cards_queue.get()

            for item in cards:
                card_object: dict = {
                    'colors': [],
                    'sizes': []
                }

                try:
                    article_data = ArticleSchema(**item).dict()
                except pydantic.ValidationError:
                    logger.critical('validation error')
                    sys.exit()

                card_object['brands'] = {
                    'id': item.get('brandId'),
                    'name': item.get('brand')
                }

                card_object['items'] = {
                    'id': item.get('root'),
                    'category': category_id,
                    'brand': item.get('brandId')
                }

                card_object['articles'] = {
                    'id': item.get('id'),
                    'item': item.get('root'),
                    'name': item.get('name')
                }

                card_object['articles_history'] = {
                    'article': item.get('id'),
                    'timestamp': self.timestamp,
                    'price_full': item.get('priceU'),
                    'price_with_discount': item.get('salePriceU'),
                    'sale': item.get('sale'),
                    'rating': item.get('rating'),
                    'feedbacks': item.get('feedbacks'),
                }

                colors: list[dict] = article_data.get('colors')
                for color in colors:
                    color_object = ColorSchema(**color)
                    card_object['colors'].append(color_object.dict())
                    card_object['articles'].update(
                        {'color': 999999} if len(colors) > 1 else
                        {'color': color.get('id')})

                sum_count: int = 0
                hash_sizes: dict = {}
                for size in item.get('sizes'):
                    size_count: int = 0
                    size_name: str = size.get('name')
                    for stock in size.get('stocks'):
                        item_count: int = stock.get('qty')
                        if item_count:
                            size_count += item_count
                    hash_sizes[size_name] = size_count
                    sum_count += size_count

                    card_object['sizes'].append({
                        'name': size_name,
                        'count': size_count
                    })

                card_object['articles_history'].update(
                    {'sum_count': sum_count})

                await self.db_queue.put(card_object)

            self.cards_queue.task_done()

            finish: float = time.time()
            impl_time: float = finish - start
            logger.info('collected data for %d: %s items in %d seconds',
                        category_id, len(card_object), impl_time)

    async def write_to_db(self, cancel_write_to_db: Event) -> None:
        while True:
            card = await self.db_queue.get()

            # your code here

            self.db_queue.task_done()

            if self.db_queue.empty():
                cancel_write_to_db.set()


async def parsing_manager(categories: list[dict]) -> None:

    parser = ItemsParser()
    semaphore = Semaphore(SEMAPHORE_LIMIT)

    get_items_ids_tasks: list[Task] = [
        create_task(parser.get_items_ids(
            category, semaphore)) for category in categories[:100]
    ]

    cancel_write_to_db = Event()

    get_cards_task = create_task(parser.get_cards())
    collect_data_task = create_task(parser.collect_data())
    write_to_db_task = create_task(parser.write_to_db(cancel_write_to_db))

    await asyncio.gather(*get_items_ids_tasks)

    await cancel_write_to_db.wait()

    get_cards_task.cancel()
    collect_data_task.cancel()
    write_to_db_task.cancel()


def load_all_items() -> None:
    start: float = time.time()

    engine = create_engine(POSTGRES_URL)

    with Session(engine) as session:
        # selectable: Select = select(Category).where(Category.id.in_([130268]))
        selectable: Select = select(Category)
        categories: list[dict] = []
        for category in session.scalars(selectable):
            category_as_dict = category.__dict__
            shard: str = category_as_dict.get('shard')
            if shard and 'blackhole' not in shard and 'preset' not in shard:
                categories.append(category_as_dict)

    asyncio.run(parsing_manager(categories))

    finish: float = time.time()
    impl_time: float = finish - start
    logger.info('done in %d seconds', impl_time)
