from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session

from mappings import ScrapedJob, Base
from mappings.utils import session_scope, install_pgcrypto, create_table, create_user, alter_table_owner

SUPERUSER = 'postgresql://postgres@localhost:5432/jobdb'
DIRK = 'postgresql://dirk_gently@localhost:5432/jobdb'

example = {
    "company": "Nings ",
    "deadline": "2017-06-13T00:00:00",
    "posted": "2017-06-06T10:15:00",
    "spider": "alfred",
    "title": "Okkur vantar sendla, ert \u00fe\u00fa r\u00e9tta manneskjan?",
    "url": "https://alfred.is/starf/11076"
}


def test_user_session():
    with session_scope(DIRK) as session:
        # don't want to do any commits just yet, so check that we get back the right type of object
        assert isinstance(session, Session)
        # and that it's configured correctly.
        # Note: calling str() on the `.url` property calls the `sqlalchemy.engine.url.URL.__to_string__`
        # method which handles creating a valid string representation of the connection credentials
        assert str(session.bind.url) == DIRK


def test_db_setup():
    # this test uses the superuser that is installed by default, `postgres` since we can only
    # install extensions as a super user. the extension is needed because the table makes use
    # of the `gen_random_uuid` function which comes from the `pgcrypto` extension.
    with session_scope(SUPERUSER) as session:
        connection = session.bind
        install_pgcrypto(connection)
        create_table(connection)
        alter_table_owner(connection, 'dirk_gently', 'scraped-jobs')


def test_insert_of_example_item():
    job = ScrapedJob.from_dict(example)

    with session_scope(DIRK) as session:
        session.add(job)
        session.commit()

        query = session.query(ScrapedJob).filter(ScrapedJob.url == example['url'])
        result = query.one()
        assert job.url == result.url == example['url']
        assert job.data == result.data == example


def test_user_creation():
    with session_scope(SUPERUSER) as session:
        query = session.bind.execute("""SELECT rolname FROM pg_roles WHERE rolname ='todd_brotzman'""")
        result = query.fetchone()
        # we should get None since the user doesn't yet exist
        assert result is None

        create_user(session.bind, 'todd_brotzman')

        query = session.bind.execute("""SELECT rolname FROM pg_roles WHERE rolname ='todd_brotzman'""")
        result = query.fetchone()
        # now we get a row containing the user
        assert result == ('todd_brotzman',)


def setup_module(module):
    engine = create_engine(SUPERUSER)
    engine.execute("""CREATE USER dirk_gently NOINHERIT;""")


def teardown_module(module):
    engine = create_engine(SUPERUSER)
    # drop the table we created
    Base.metadata.drop_all(engine)
    # and remove the extension
    engine.execute("""DROP EXTENSION IF EXISTS pgcrypto;""")
    # finally, clean up the created user
    engine.execute("""DROP ROLE IF EXISTS dirk_gently;""")
    engine.execute("""DROP ROLE IF EXISTS todd_brotzman;""")
