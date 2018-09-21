import unittest
def alwaysTrue():
    return 'dcbnashjk'

class TestAlwaysTrue(unittest.TestCase):
    def test_assertTrue(self):
        result = alwaysTrue()
        self.assertTrue(result)

    def test_assertIs(self):
        result = alwaysTrue()
        self.assertIs(result, True)

    def test_assertFalse(self):
        result = alwaysTrue()
        self.assertFalse(result, True)