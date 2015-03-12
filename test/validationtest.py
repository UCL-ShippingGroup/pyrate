import unittest
from pyrate import utils

class TestValidationFunctions(unittest.TestCase):

    def test_valid_mmsi(self):
        self.assertFalse(utils.valid_mmsi(None))
        self.assertFalse(utils.valid_mmsi(0))

    def test_valid_imo(self):
        # basic invalid inputs
        self.assertFalse(utils.valid_imo(None))
        self.assertFalse(utils.valid_imo(0))
        self.assertFalse(utils.valid_imo('0'))

        # invalid imos
        for imo in [1000000, 9999999, 5304985]:
            self.assertFalse(utils.valid_imo(imo))
            self.assertFalse(utils.valid_imo(str(imo)))

        # valid imos
        for imo in [7654329, 8137249, 9404584, 9281011, 9328522, 9445590]:
            self.assertTrue(utils.valid_imo(imo))
            self.assertTrue(utils.valid_imo(str(imo)))

if __name__ == '__main__':
    unittest.main()
