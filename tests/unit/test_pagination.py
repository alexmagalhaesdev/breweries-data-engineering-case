from app.tasks.extract import parse_last_page

def test_parse_last_page_happy():
    link = '<https://api...?page=2>; rel="next", <https://api...?page=17>; rel="last"'
    assert parse_last_page(link) == 17

def test_parse_last_page_missing():
    assert parse_last_page(None) is None

def test_parse_last_page_malformed():
    assert parse_last_page('rel="next"') is None
