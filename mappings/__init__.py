import dateutil.parser
from sqlalchemy import Column, Text, event, cast
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP
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
        instance = cls()
        if 'url' in input_dict:
            instance.url = input_dict['url']
        instance.data = input_dict
        return instance

    @hybrid_property
    def posted(self):
        if self.data is None or 'posted' not in self.data:
            return None
        return dateutil.parser.parse(self.data['posted'])

    @posted.expression
    def posted(cls):
        return cast(cls.data.op('->>')('posted'), TIMESTAMP(timezone=True))

    @hybrid_property
    def spider(self):
        if self.data is None or 'spider' not in self.data:
            return None
        return self.data['spider']

    @spider.expression
    def spider(cls):
        return cls.data.op('->>')('spider')


@event.listens_for(ScrapedJob, 'before_update', propagate=True)
def update_last_modified_timestamp(mapper, connection, row):
    # updates the value of the `last_modified` column when we're doing an UPDATE
    row.last_modified = func.now()
