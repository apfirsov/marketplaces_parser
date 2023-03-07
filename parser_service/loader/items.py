import asyncio
import json
import sys
import time
from asyncio import Queue, Semaphore, Task, create_task
import datetime

import pydantic
from aiohttp import ClientSession
from db.models import Category
from db.session import get_db
from logger_config import parser_logger as logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.selectable import Select
from db.models import (
    Color,
    Brand,
    Item,
    Article,
    ArticlesHistory,
    Size,
    HistorySizeRelation
)

from constants import (ATTEMPTS_COUNTER, BASE_URL, LAST_PAGE_TRESHOLD,
                        MAX_BRANDS_IN_REQUEST, MAX_ITEMS_IN_BRANDS_FILTER,
                        MAX_ITEMS_IN_REQUEST, MAX_PAGE, MIN_PRICE_RANGE,
                        QUERY_PARAMS, REQUEST_LIMIT, WORKER_COUNT)
from schemas import ArticleSchema, ColorSchema

items_count: int = 0

# TODO: Синхронные логи (асинк или сократить количество логирования)
# TODO: Проверить алгоритмы фильтрации
# TODO: Замеры в декораторе
# TODO: Код стайл
# TODO: Добавить БД
# TODO: Найти/сократить синхронные вставки
# TODO: После выполнения пунктов выше замеры, подбор параметров


class ItemsParser:

    def __init__(self, client_session: ClientSession) -> None:
        self._session: ClientSession = client_session
        self._timestamp: float = datetime.datetime.now()
        self._request_semaphore = Semaphore(REQUEST_LIMIT)
        self._categories_queue: Queue = Queue()
        self._ids_queue: Queue = Queue()
        self._cards_queue: Queue = Queue()
        self._db_queue: Queue = Queue()
        self._queues = (
            self._categories_queue,
            self._ids_queue,
            self._cards_queue,
            self._db_queue,
        )
        self._req_counter = 0

    async def start(self, categories):
        for category in categories.scalars():
            category_as_dict = category.__dict__
            shard: str = category_as_dict.get('shard')
            if shard and 'blackhole' not in shard and 'preset' not in shard:
                self._categories_queue.put_nowait(category_as_dict)

        for _ in range(WORKER_COUNT):
            create_task(self._get_cards())
            create_task(self._collect_data())
            create_task(self._get_items_ids())
        create_task(self._write_to_db())

        await create_task(self._waiter())

    async def _waiter(self) -> None:

        while True:
            for queue in self._queues:
                await queue.join()

            if all([queue.empty() for queue in self._queues]):
                break

    async def _get_data(self, url: str) -> dict:

        async with self._request_semaphore:
            attempts_counter: int = ATTEMPTS_COUNTER

            while attempts_counter:
                try:
                    async with self._session.get(url, ssl=False) as response:
                        if response.ok:
                            self._req_counter += 1
                            return await response.json(content_type=None)

                        logger.info('Bad response status %d at: %s',
                                    response.status, url)

                # except (json.decoder.JSONDecodeError, AttributeError) as err:
                except Exception as err:
                    logger.info('request error occured at: %s, %s', url, err)

                logger.info('request at: %s, %d tries left',
                            url, attempts_counter)
                attempts_counter -= 1
                await asyncio.sleep(ATTEMPTS_COUNTER-attempts_counter)
                if not attempts_counter:
                    logger.critical('attempts_counter lost at: %s', url)
                    sys.exit()


    async def _get_items_ids(self) -> None:

        category = await self._categories_queue.get()

        start: float = time.time()
        category_id: int = category.get('id')
        shard: str = category.get('shard')
        query: str = category.get('query')
        price_filter_url: str = (f'{BASE_URL}{shard}/v4/'
                                 f'filters?{query}{QUERY_PARAMS}')

        response = await self._get_data(price_filter_url)

        ctg_filters: list[dict] = (
            response.get('data').get('filters'))
        for ctg_filter in ctg_filters:
            if ctg_filter.get('key') == 'priceU':
                ctg_max_price: int = ctg_filter.get('maxPriceU')  # !!!!!!
                break

        await self._basic_parsing(
            category_id, shard, query, 0, ctg_max_price)

        self._categories_queue.task_done()

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

        response = await self._get_data(last_page_url)
        response_data: list[dict] = (
            response.get('data').get('products'))

        last_page_is_full: bool = (
            len(response_data) > LAST_PAGE_TRESHOLD)

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

        response = await self._get_data(brand_filter_url)
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
        # кажется, это тут не нужно
        # TODO: Разобраться
        # concatenated_ids_list.append(concatenated_ids[1:])

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

    async def _get_items_ids_chunk(
            self, category_id: int, base_url: str) -> None:

        logger.info('getting items ids chunks')

        page: int = 1
        concatenated_ids: str = ''
        cnt: int = 0

        while page <= MAX_PAGE:
            url: str = base_url + '&page=' + str(page)

            response = await self._get_data(url)
            response_data: list[dict] = response.get('data').get('products')

            if not len(response_data):
                # logger.info('category %d, cnt %d !!1!!', category_id, cnt)
                self._ids_queue.put_nowait((category_id, concatenated_ids))
                break

            for item in response_data:
                item_id: int = item.get('id')
                if cnt < MAX_ITEMS_IN_REQUEST:
                    concatenated_ids += (';', '')[len(
                        concatenated_ids) == 0] + str(item_id)
                    cnt += 1
                else:
                    # logger.info('category %d, cnt %d !!2!!', category_id, cnt)
                    self._ids_queue.put_nowait(
                        (category_id, concatenated_ids))
                    concatenated_ids = str(item_id)
                    cnt = 1
            page += 1

    async def _get_cards(self) -> None:
        while True:
            category_id: int
            concatenated_ids: str
            category_id, concatenated_ids = await self._ids_queue.get()

            base_url: str = (f'https://card.wb.ru/cards/detail?'
                             f'spp=30{QUERY_PARAMS}&nm=')

            url: str = base_url + concatenated_ids

            response = await self._get_data(url)
            response_data: list[dict] = (
                response.get('data').get('products'))

            # logger.info('category %d, len_response_data %d -- get_cards',
            #             category_id, len(response_data))

            self._cards_queue.put_nowait((category_id, response_data))

            self._ids_queue.task_done()

            logger.info(f'got cards chunk for {category_id}')

    async def _collect_data(self) -> None:
        while True:
            category_id: int
            cards: list[dict]
            category_id, cards = await self._cards_queue.get()
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
                    'timestamp': self._timestamp,
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

                self._db_queue.put_nowait(card_object)

            self._cards_queue.task_done()

            logger.info('collected data for %d: %s items',
                        category_id, len(cards))

    async def _simple_create(self, entity, in_data, s) -> None:
        item_in_db = await s.get(entity, in_data["id"])
        if item_in_db is None:
            s.add(entity(**in_data))

    async def _write_to_db(self) -> None:
        while True:
            card = await self._db_queue.get()

            global items_count
            items_count += 1
            if items_count % 100000 == 0:
                logger.critical('ITEMS COUNT <<< %d >>>', items_count)

            db = get_db()
            session: AsyncSession = await anext(db)

            colors: list = card["colors"]
            sizes: list = card["sizes"]
            brands: dict = card["brands"]
            items: dict = card["items"]
            articles: dict = card["articles"]
            articles_history: dict = card["articles_history"]

            async with session.begin():
                try:
                    # TODO Убрать цикл после доработки от Саши
                    for color in colors:
                        await self._simple_create(Color, color, session)

                    await self._simple_create(Brand, brands, session)
                    await self._simple_create(Item, items, session)
                    await self._simple_create(Article, articles, session)

                    history_in_db = ArticlesHistory(**articles_history)
                    session.add(history_in_db)

                    # size in db
                    # TODO Доработать запись в БД Size
                    db_sizes = {}
                    for size in sizes:
                        res = await session.scalars(
                            select(Size).where(Size.name == size["name"]))
                        size_in_db = res.one_or_none()
                        if size_in_db is None:
                            size_in_db = Size(name=size["name"])
                            session.add(size_in_db)
                        db_sizes[size["name"]] = (size["count"], size_in_db)
                    await session.flush()

                    # history_size_relation in db
                    for count, size_in_db in db_sizes.values():
                        session.add(
                            HistorySizeRelation(
                                history=history_in_db.id,
                                size=size_in_db.id,
                                count=count
                            )
                        )

                except Exception as err:
                    await session.rollback()
                    logger.critical("!!!!!!Error write_to_db!!!! %s", err)
                    raise err
                else:
                    await session.commit()
            self._db_queue.task_done()


async def load_all_items() -> None:
    start: float = time.time()

    db = get_db()
    session: AsyncSession = await anext(db)

    async with session.begin():
        # selectable: Select = select(Category).where(Category.id.in_([63010]))
        # selectable: Select = select(Category)
        selectable: Select = select(Category).where(Category.id.in_([130558]))
        categories = await session.execute(selectable)

    async with ClientSession() as client_session:
        parser = ItemsParser(client_session)

        await parser.start(categories)


    finish: float = time.time()
    impl_time: float = finish - start
    logger.info('got %d items in %d seconds, %d requests', items_count, impl_time, parser._req_counter)

# 130267 3399
# 130274 1540
# 130268 194
