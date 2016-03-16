import unittest
import pep8


class TestCodeFormat(unittest.TestCase):

    def test_pep8_conformance(self):
        """Test that we conform to PEP8."""
        pep8style = pep8.StyleGuide(quiet=False)
        pep8style.options.ignore = pep8style.options.ignore + ("E501",)
        result = pep8style.check_files(['dbt', 'test'])
        self.assertEqual(result.total_errors, 0, 'Found code style errors (and warnings).')
