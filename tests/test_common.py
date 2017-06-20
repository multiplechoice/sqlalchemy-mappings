import datetime

from mappings import ScrapedJob

expected_item_with_datetimes = {
    "company": "Nings ",
    "deadline": "2017-06-13T00:00:00",
    "posted": "2017-06-06T10:15:00",
    "spider": "alfred",
    "title": "Okkur vantar sendla, ert \u00fe\u00fa r\u00e9tta manneskjan?",
    "url": "https://alfred.is/starf/11076"
}

expected_item_with_date = {
    "company": "Tryggingami\u00f0lun \u00cdslands",
    "posted": "2017-06-06",
    "spider": "alfred",
    "title": "Fr\u00e1b\u00e6rt aukastarf \u00ed bo\u00f0i",
    "url": "https://alfred.is/starf/11077"
}

expected_item_without_url = {
    "company": "S\u00e6ta Sv\u00edni\u00f0 ",
    "deadline": "2017-11-06T00:00:00",
    "posted": "2017-06-06T10:31:00",
    "spider": "alfred",
    "title": "Leitum af uppvaskara / looking for dishwasher",
}


def test_expected_invocation():
    job = ScrapedJob.from_dict(expected_item_with_datetimes)
    # created_at will be extracted from the input dict
    assert isinstance(job.created_at, datetime.datetime)
    assert job.created_at.isoformat() == '2017-06-06T10:15:00'
    # we won't have a last_modified time yet since we've not written it to the db
    assert job.last_modified is None
    assert job.url == 'https://alfred.is/starf/11076'
    assert job.data == expected_item_with_datetimes
    assert job.id is None


def test_expected_when_posted_is_a_date():
    job = ScrapedJob.from_dict(expected_item_with_date)
    assert isinstance(job.created_at, datetime.datetime)
    assert job.created_at.isoformat() == '2017-06-06T00:00:00'
    assert job.last_modified is None
    assert job.data == expected_item_with_date
    assert job.id is None


def test_exected_when_url_is_missing():
    job = ScrapedJob.from_dict(expected_item_without_url)
    assert job.url is None


def test_expected_when_item_is_empty():
    job = ScrapedJob.from_dict({})
    assert job.data == {}
    assert job.url is None
    assert job.created_at is None
    assert job.last_modified is None
    assert job.id is None


def test_init_method():
    job = ScrapedJob()
    assert job.data is None
    assert job.url is None
    assert job.created_at is None
    assert job.last_modified is None
    assert job.id is None
