import unittest
from hadoop import *
import StringIO

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


    def test_should_parse_country_dictionary(self):
        row = '"34643712","34644479","RU","Russian Federation"'
        rec = UsersByCountry.parse_location_row(row)

        self.assertEqual(rec['lo'], 34643712)
        self.assertEqual(rec['hi'], 34644479)
        self.assertEqual(rec['country'], 'RU')


    def test_should_encode_ip(self):
        self.assertEqual(34643712, UsersByCountry.ip2code('2.16.159.0'))

    def test_it_should_init_mapper(self):

        country_dict_stub = StringIO.StringIO(
                '"34646016","34646271","RU","Russian Federation"\n'
                '"34643712","34644479","RU","Russian Federation"\n'
                '"34644480","34644991","DE","Germany"\n'
                '"34644992","34645503","US","United States"\n'
                '"34645504","34646015","NL","Netherlands"\n'
                '"34646272","34646527","NL","Netherlands"\n'
        )

        job = UsersByCountry(date=None)
        job.init_mapper(country_dict_stub)

        self.assertEqual(sorted([34643712, 34644480, 34644992, 34645504, 34646016, 34646272]), job.locations_begs)
        self.assertEqual(sorted([34643712, 34644480, 34644992, 34645504, 34646016, 34646272]), [x['lo'] for x in job.locations])

    def test_it_should_find_ip(self):
        country_dict_stub = StringIO.StringIO(
                '"34646016","34646271","RU","Russian Federation"\n'
                '"34643712","34644479","RU","Russian Federation"\n'
                '"34644480","34644991","DE","Germany"\n'
                '"34644992","34645503","US","United States"\n'
                '"34645504","34646015","NL","Netherlands"\n'
                '"34646272","34646527","NL","Netherlands"\n'
        )

        job = UsersByCountry(date=None)
        job.init_mapper(country_dict_stub)

        self.assertEqual('DE', job.find_country('2.16.162.42'))
        self.assertEqual('RU', job.find_country('2.16.168.42'))

if __name__ == '__main__':
    unittest.main()