import unittest
from pretty_pipeline import *

def add_value_to_list(lst, val):
    return [el +  val for el in lst]


class TestPipeline(unittest.TestCase):

    def test_pipeline(self):
        """Test basic pipeline with a single Transform step."""
        data = [1, 2, 3]
        pipeline = Pipeline(
            data=data,
            steps=[
                Transform(add_value_to_list,
                          1)
            ]
        )
        transformed = pipeline.run()

        self.assertEqual(transformed, [2,3,4])


if __name__ == "__main__":
    unittest.main()
