import dateutil.parser
from sqlalchemy import Column, Text, event, cast
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func

Base = declarative_base()


class ScrapedJob(Base):
    __tablename__ = 'scraped-jobs'
    id = Column(UUID, server_default=func.gen_random_uuid())
    url = Column(Text, nullable=False, primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
    last_modified = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
    data = Column(JSONB, nullable=False)

    def __repr__(self):
        return '<Job created_at: {self.created_at}' \
               ' last_modified: {self.last_modified}' \
               ' url: {self.url!r}' \
               '>'.format(self=self)

    @classmethod
    def from_dict(cls, input_dict):
        """
        Used to instantiate a `ScrapedJob` instance from the data yielded by a Scrapy spider.
        Args:
            input_dict (dict): the Scrapy item

        Returns:
            obj:`ScrapedJob`: instance of the `ScrapedJob` class

        """
        instance = cls()
        if 'url' in input_dict:
            instance.url = input_dict['url']
        instance.data = input_dict
        return instance

    @hybrid_property
    def posted(self):
        """
        Extracts the timestamp of the posting from the data blob. If it is missing, or the data blob hasn't been set
        we get None instead.

        Returns:
            datetime.datetime or None: posting date

        """
        if self.data is None or 'posted' not in self.data:
            return None
        return dateutil.parser.parse(self.data['posted'])

    @posted.expression
    def posted(cls):
        return cast(cls.data.op('->>')('posted'), TIMESTAMP(timezone=True))

    @hybrid_property
    def date_of_posting(self):
        """
        Returns the date of the posting, as a YYYY-MM-DD string.

        Returns:
            str: date of posting

        """
        posting = self.posted
        if posting is not None:
            return posting.date()
        return None

    @date_of_posting.expression
    def date_of_posting(cls):
        return cast(func.date_trunc('day', cls.posted), DATE)

    @hybrid_property
    def spider(self):
        """
        Extracts the spider name from the data blob. If the spider field isn't present, or there is not data blob,
        we get a None instead.

        Returns:
            basestring or None: the name of the spider

        """
        if self.data is None or 'spider' not in self.data:
            return None
        return self.data['spider']

    @spider.expression
    def spider(cls):
        return cls.data.op('->>')('spider')

    def as_dict(self):
        """
        Flattens the fields of the instance into a single dict. This is useful for returning the data in the API,
        since all fields are included.

        Returns:
            dict: all the fields in the `ScrapedJob` instance, flattened

        """
        contents = dict()

        if self.data:
            contents.update(self.data)

        for key in ('created_at', 'last_modified'):
            contents[key] = value = getattr(self, key)
            if value is not None:
                contents[key] = getattr(self, key).isoformat()

        contents['id'] = self.id

        return contents


@event.listens_for(ScrapedJob, 'before_update', propagate=True)
def update_last_modified_timestamp(mapper, connection, row):
    # updates the value of the `last_modified` column when we're doing an UPDATE
    row.last_modified = func.now()
