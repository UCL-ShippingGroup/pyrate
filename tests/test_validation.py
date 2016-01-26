from pyrate.utils import valid_imo, \
                         valid_mmsi, \
                         is_valid_cog, \
                         is_valid_heading, \
                         is_valid_sog, \
                         valid_latitude, valid_longitude

import numpy as np

class TestValidationFunctions():

    def test_valid_mmsi(self):
        assert valid_mmsi(None) == False
        assert valid_mmsi(0) == False

    def test_imo_basic_invalid_inputs(self):
        assert valid_imo(None) == False
        assert valid_imo(0) == False
        assert valid_imo('0') == False

    def test_imo_invalid_imos(self):
        for imo in [1000000, 9999999, 5304985]:
            assert valid_imo(imo) == False
            assert valid_imo(str(imo)) == False

    def test_imo_valid_imos(self):
        for imo in [7654329, 8137249, 9404584, 9281011, 9328522, 9445590]:
            assert valid_imo(imo) == True
            assert valid_imo(str(imo)) == True

    def test_heading_with_none(self):
        """Don't raise an error if the heading is not available
        """
        heading = None
        assert is_valid_heading(heading) == False

    def test_valid_heading(self):
        for heading in np.linspace(0, 359.99, 100):
            assert is_valid_heading(heading) == True
        heading = 360
        assert is_valid_heading(heading) == False

    def test_valid_coarse_over_ground(self):
        for course in np.linspace(0, 359.99, 100):
            assert is_valid_cog(course)
        course = 360
        assert is_valid_cog(course) == False

    def test_valid_speed_over_ground(self):
        for speed in np.linspace(0, 102.2, 100):
            assert is_valid_sog(speed)
        negative_speed = -1
        assert is_valid_sog(negative_speed) == False
        too_high_speed = 102.4
        assert is_valid_sog(too_high_speed) == False

    def test_valid_longitude(self):
        for angle in np.linspace(-180, 180, 100):
            assert valid_longitude(angle) == True
        for angle in [-181, 181]:
            assert valid_longitude(angle) == False

    def test_valid_latitude(self):
        for angle in np.linspace(-90, 90, 100):
            assert valid_latitude(angle) == True
        for angle in [-91, 91]:
            assert valid_latitude(angle) == False
