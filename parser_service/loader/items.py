import asyncio
import json
import sys
import time
from asyncio import Event, Queue, Semaphore, Task, create_task

import aiohttp
import pydantic
from db.models import Category
from db.session import get_db
from logger_config import parser_logger as logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import Select

from constants import (ATTEMPTS_COUNTER, BASE_URL, LAST_PAGE_TRESHOLD,
                        MAX_BRANDS_IN_REQUEST, MAX_ITEMS_IN_BRANDS_FILTER,
                        MAX_ITEMS_IN_REQUEST, MAX_PAGE, MIN_PRICE_RANGE,
                        QUERY_PARAMS, SEMAPHORE_LIMIT)
from schemas import ArticleSchema, ColorSchema

items_count: int = 0


class ItemsParser:

    def __init__(self) -> None:
        self.timestamp: float = time.time()
        self.semaphore = Semaphore(SEMAPHORE_LIMIT)
        # self._categories_empty = False
        self.complete = Event()

        self.categories_queue: Queue = Queue()
        self.ids_queue: Queue = Queue()
        self.cards_queue: Queue = Queue()
        self.db_queue: Queue = Queue()

    async def start(self, categories):
        for category in categories.scalars():
            category_as_dict = category.__dict__
            shard: str = category_as_dict.get('shard')
            if shard and 'blackhole' not in shard and 'preset' not in shard:
                self.categories_queue.put_nowait(category_as_dict)

        await self.parsing_manager()

    async def parsing_manager(self) -> None:

        tasks_list: list[Task] = [
            create_task(self.get_items_ids())
            for _ in range(self.categories_queue.qsize())
        ]

        await asyncio.gather(*tasks_list)

        create_task(self.get_cards())
        create_task(self.collect_data())
        create_task(self.write_to_db())

        # tasks_list.append(create_task(self.waiter()))
        waiter_task = create_task(self.waiter())

        await waiter_task

    async def waiter(self):
        while True:
            print('начинаем ждать')
            await self.complete.wait()
            print('дождались')
            if (self.categories_queue.empty()
                and self.ids_queue.empty()
                and self.cards_queue.empty()
                    and self.db_queue.empty()):
                print('очереди пустые')
                break
            else:
                self.complete.clear()

    async def get_items_ids(self) -> None:

        async with self.semaphore:

            category = await self.categories_queue.get()

            start: float = time.time()
            category_id: int = category.get('id')
            shard: str = category.get('shard')
            query: str = category.get('query')
            price_filter_url: str = (f'{BASE_URL}{shard}/v4/'
                                     f'filters?{query}{QUERY_PARAMS}')

            attempts_counter: int = ATTEMPTS_COUNTER
            while attempts_counter:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(price_filter_url, ssl=False) as r:
                            response: dict = await r.json(content_type=None)

                    ctg_filters: list[dict] = (
                        response.get('data').get('filters'))
                    for ctg_filter in ctg_filters:
                        if ctg_filter.get('key') == 'priceU':
                            ctg_max_price: int = ctg_filter.get('maxPriceU')  # !!!!!!
                            break
                    break

                except (json.decoder.JSONDecodeError, AttributeError) as error:
                    attempts_counter -= 1
                    if not attempts_counter:
                        logger.critical(error)
                        sys.exit()
                    logger.info(
                        'request error occured at: %s, %d tries left',
                        price_filter_url, attempts_counter
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
        attempts_counter: int = ATTEMPTS_COUNTER

        while attempts_counter:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(last_page_url, ssl=False) as r:
                        response: dict = await r.json(content_type=None)

                response_data: list[dict] = (
                    response.get('data').get('products'))

                last_page_is_full: bool = (
                    len(response_data) > LAST_PAGE_TRESHOLD)
                break

            except (json.decoder.JSONDecodeError, AttributeError) as error:
                attempts_counter -= 1
                if not attempts_counter:
                    logger.critical(error)
                    sys.exit()
                logger.info('request error at: %s, %d tries left',
                            last_page_url, attempts_counter)

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

        attempts_counter: int = ATTEMPTS_COUNTER

        while attempts_counter:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(brand_filter_url, ssl=False) as r:
                        response: dict = await r.json(content_type=None)

                brand_filters: list[dict] = (
                    response.get('data').get('filters')[0].get('items'))

            except (json.decoder.JSONDecodeError,
                    AttributeError, TypeError) as error:
                attempts_counter -= 1
                if not attempts_counter:
                    logger.critical(error)
                    sys.exit()
                logger.info('request error at: %s, %d tries left',
                            brand_filter_url, attempts_counter)

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
            attempts_counter: int = ATTEMPTS_COUNTER
            concatenated_ids: str = ''
            cnt: int = 0

            while page <= MAX_PAGE:
                url: str = base_url + '&page=' + str(page)
                try:
                    async with session.get(url, ssl=False) as response:
                        response: dict = await response.json(content_type=None)

                    response_data: list[dict] = (
                        response.get('data').get('products'))

                    if not len(response_data):
                        logger.info('category %d, cnt %d !!1!!', category_id, cnt)
                        self.ids_queue.put_nowait(
                            (category_id, concatenated_ids))
                        # if self.categories_queue.empty():
                            # self.complete.set()
                        break

                    for item in response_data:
                        item_id: int = item.get('id')
                        if cnt < MAX_ITEMS_IN_REQUEST:
                            concatenated_ids += (';', '')[len(
                                concatenated_ids) == 0] + str(item_id)
                            cnt += 1
                        else:
                            logger.info('category %d, cnt %d !!2!!', category_id, cnt)
                            self.ids_queue.put_nowait(
                                (category_id, concatenated_ids))
                            # if self.categories_queue.empty():
                            #     self.complete.set()
                            concatenated_ids = str(item_id)
                            cnt = 1

                    attempts_counter = 0
                    page += 1
                except (json.decoder.JSONDecodeError, AttributeError) as error:
                    attempts_counter -= 1
                    if not attempts_counter:
                        logger.critical(error)
                        sys.exit()
                    logger.info('request error at: %s, %d tries left',
                                url, attempts_counter)

    async def get_cards(self) -> None:
        while True:
            if self.categories_queue.empty() and self.ids_queue.empty():
                self.complete.set()
            category_id: int
            concatenated_ids: str
            category_id, concatenated_ids = await self.ids_queue.get()

            base_url: str = (f'https://card.wb.ru/cards/detail?'
                             f'spp=30{QUERY_PARAMS}&nm=')

            async with aiohttp.ClientSession() as session:
                url: str = base_url + concatenated_ids

                async with session.get(url, ssl=False) as response:
                    response: dict = await response.json(content_type=None)
                    response_data: list[dict] = (
                        response.get('data').get('products'))

            logger.info('category %d, len_response_data %d -- get_cards',
                        category_id, len(response_data))

            self.cards_queue.put_nowait((category_id, response_data))

            logger.info(f'got cards chunk for {category_id}')

    async def collect_data(self) -> None:
        while True:
            if self.ids_queue.empty() and self.cards_queue.empty():
                self.complete.set()
            category_id: int
            cards: list[dict]
            category_id, cards = await self.cards_queue.get()
            a = 1
            for item in cards:

                card_object: dict = {
                    'colors': [],
                    'sizes': []
                }

                try:
                    article_data = ArticleSchema(**item).dict()
                except pydantic.ValidationError:
                    logger.critical('validation error at article %d',
                                    item.get('id'))
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

                self.db_queue.put_nowait(card_object)

            logger.info('collected data for %d: %s items',
                        category_id, len(cards))

    async def write_to_db(self) -> None:
        while True:
            if self.cards_queue.empty() and self.db_queue.empty():
                self.complete.set()
            card = await self.db_queue.get()

            global items_count
            items_count += 1

            # your code here


async def load_all_items() -> None:
    start: float = time.time()

    db = get_db()
    session: AsyncSession = await anext(db)

    async with session.begin():
        selectable: Select = select(Category).where(Category.id.in_([130267, 130274]))
        # selectable: Select = select(Category)
        categories = await session.execute(selectable)
        parser = ItemsParser()
        await parser.start(categories)

    finish: float = time.time()
    impl_time: float = finish - start
    logger.info('got %d items in %d seconds', items_count, impl_time)

# 130267 3399
# 130274 1540
# 130268 194
