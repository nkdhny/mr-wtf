import unittest
from map import parse_line

class TestMapParsesLine(unittest.TestCase):

    def test_should_parse_ip(self):
        l = '195.225.111.113 - - [13/Oct/2016:00:00:00 +0400] "GET /id45037 HTTP/1.1" 200 26236 "-" "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36"'
        rec = parse_line(l)
        self.assertEqual(rec['ip'], '195.225.111.113')

    def test_should_parse_date(self):
        l = '195.225.111.113 - - [13/Oct/2016:00:00:00 +0400] "GET /whatever.html?like=1 HTTP/1.1" 200 26236 "-" "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36"'
        rec = parse_line(l)
        self.assertEqual(rec['epoch'], 1476316800)

        self.assertTrue(rec['profile'] is None)
        self.assertTrue(rec['like'] is None)

    def test_should_parse_profile(self):
        l = '195.225.111.113 - - [13/Oct/2016:00:00:00 +0400] "GET /id45037 HTTP/1.1" 200 26236 "-" "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36"'
        rec = parse_line(l)
        self.assertEqual(rec['epoch'], 1476316800)

        self.assertEqual(rec['profile'], 45037)
        self.assertFalse(rec['like'])

    def test_should_parse_like(self):
        l = '195.225.111.113 - - [13/Oct/2016:00:00:00 +0400] "GET /id45037?like=1 HTTP/1.1" 200 26236 "-" "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36"'
        rec = parse_line(l)
        self.assertEqual(rec['epoch'], 1476316800)

        self.assertEqual(rec['profile'], 45037)
        self.assertTrue(rec['like'])


if __name__ == '__main__':
    unittest.main()