import dateutil.parser
from sqlalchemy import Column, Text, event
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
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
        if 'posted' in input_dict:
            instance.created_at = dateutil.parser.parse(input_dict['posted'])
        instance.data = input_dict
        return instance


@event.listens_for(ScrapedJob, 'before_update', propagate=True)
def update_last_modified_timestamp(mapper, connection, row):
    # updates the value of the `last_modified` column when we're doing an UPDATE
    row.last_modified = func.now()
