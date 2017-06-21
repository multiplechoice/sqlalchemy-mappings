import datetime

from sqlalchemy import create_engine

from mappings import ScrapedJob, Base
from mappings.utils import session_scope, install_pgcrypto, create_user, create_table, alter_table_owner

SUPERUSER = 'postgresql://postgres@localhost:5432/jobdb'
FARAH = 'postgresql://farah_black@localhost:5432/jobdb'

example_data = [
    # regular scraped data
    {
        'url': 'https://example.com/job/1/',
        'spider': 'tarantula',
        'posted': '2017-01-01T09:35:07',
        'title': 'Do you love spiders?',
        'company': 'London Zoo'
    },
    # scraped item with the spider missing
    {
        'url': 'https://example.com/job/2/',
        'posted': '2017-02-12T12:17:53',
        'title': 'Do you want to goto space?',
        'company': 'Lunar Industries'
    },
    # scraped item with empy string for the spider
    {
        'url': 'https://example.com/job/3/',
        'spider': '',
        'posted': '2004-09-20T16:45:22',
        'title': 'DO you enjoy travelling? Come fly with us!',
        'company': 'Oceanic Airlines'
    }
]


def test_spider_and_posted():
    with session_scope(FARAH) as session:
        query = session.query(ScrapedJob).filter(ScrapedJob.url == 'https://example.com/job/1/')
        job = query.one()
        assert job.spider == 'tarantula'
        assert job.posted == datetime.datetime(2017, 1, 1, 9, 35, 7)

        query = session.query(ScrapedJob).filter(ScrapedJob.url == 'https://example.com/job/2/')
        job = query.one()
        assert job.spider is None
        assert job.posted == datetime.datetime(2017, 2, 12, 12, 17, 53)

        query = session.query(ScrapedJob).filter(ScrapedJob.url == 'https://example.com/job/3/')
        job = query.one()
        assert job.spider == ''
        assert job.posted == datetime.datetime(2004, 9, 20, 16, 45, 22)


def test_find_by_spider():
    with session_scope(FARAH) as session:
        assert session.query(ScrapedJob.data).filter(ScrapedJob.spider == 'tarantula').one()[0] == example_data[0]
        assert session.query(ScrapedJob.data).filter(ScrapedJob.spider == None).one()[0] == example_data[1]
        assert session.query(ScrapedJob.data).filter(ScrapedJob.spider == '').one()[0] == example_data[2]


def setup_module(module):
    # connect as superuser and setup the db
    engine = create_engine(SUPERUSER)
    install_pgcrypto(engine)
    create_table(engine)
    create_user(engine, 'farah_black')
    alter_table_owner(engine, 'farah_black', 'scraped-jobs')
    Base.metadata.create_all(engine)

    # now connect as Farah and insert a bunch of stuff
    with session_scope(FARAH) as session:
        for example in example_data:
            job = ScrapedJob.from_dict(example)
            session.add(job)


def teardown_module(module):
    engine = create_engine(SUPERUSER)
    # drop the table we created
    Base.metadata.drop_all(engine)
    # and remove the extension
    engine.execute("""DROP EXTENSION IF EXISTS pgcrypto;""")
    # finally, clean up the created user
    engine.execute("""DROP ROLE IF EXISTS farah_black;""")
