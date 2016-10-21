import unittest
from hadoop import *

class TestMetrics(unittest.TestCase):

    def test_should_parse_code(self):
        l = '195.225.111.113 - - [13/Oct/2016:00:00:00 +0400] "GET /id45037 HTTP/1.1" 200 26236 "-" "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36"'
        rec = parse_line(l)
        self.assertEqual(rec['code'], 200)

        l = '195.225.111.113 - - [13/Oct/2016:00:00:00 +0400] "GET /id45037 HTTP/1.1" 404 26236 "-" "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36"'
        rec = parse_line(l)
        self.assertEqual(rec['code'], 404)

    def test_should_parse_ip(self):
        l = '195.225.111.113 - - [13/Oct/2016:00:00:00 +0400] "GET /id45037 HTTP/1.1" 200 26236 "-" "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.99 Safari/537.36"'
        rec = parse_line(l)
        self.assertEqual(rec['ip'], '195.225.111.113')

if __name__ == '__main__':
    unittest.main()