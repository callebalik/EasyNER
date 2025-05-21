import unittest
import warnings

warnings.warn(
    "This test module is deprecated and may be removed in future releases.",
    DeprecationWarning,
    stacklevel=2,
)

from scripts.db_easyner import EasyNerDB


class TestDBAnalysis(unittest.TestCase):
    successful_tests = []

    def setUp(self):
        self.test_db_path = "test_database.db"
        self.db = EasyNerDB(self.test_db_path)
        self.cursor = self.db.cursor
        self.entity1 = "disease"
        self.entity2 = "phenomenon"
        self.maxDiff = None

    def tests_calc_article_lengths(self):
        self.db.calc_all_article_lengths()


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBAnalysis("tests_calc_article_lengths"))
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    result = runner.run(suite())
    if not result.wasSuccessful():
        print("\nFailed Tests:")
        for failed_test, traceback in result.failures:
            print(f" - {failed_test.id()}")
        print("\nErrored Tests:")
        for errored_test, traceback in result.errors:
            print(f" - {errored_test.id()}")

        print("\nSuccessful Tests:")
        for test_name in TestDBAnalysis.successful_tests:
            print(f" - {test_name}")
