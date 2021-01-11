import os
import unittest
from typing import List
from pretty_pipeline import *

def add_value_to_list(lst:List[int], val:int):
    return [el +  val for el in lst]


def extract_data(source:str):
    """Read list of newline delineated ints from text file."""
    with open(source, "r") as f:
        data = f.readlines()
        data = [int(el) for el in data if el != "/n"]

    return data


def load_data(data:List[int], destination:str):
    """Save list of ints to text file, delineated with newlines."""
    with open(destination, "w") as f:
        for el in data:
            f.write(str(el) + "\n")


class TestPipeline(unittest.TestCase):

    def test_pipeline(self):
        """Test basic pipeline with a single Transform step."""
        pipeline = Pipeline(
            data=[1,2,3],
            steps=[
                Transform(add_value_to_list,
                          1)
            ]
        )
        transformed = pipeline.run()

        self.assertEqual(transformed, [2,3,4])

    def test_load_extract_pipeline(self):
        """Test basic pipeline with Extract, Load and Transform steps."""
        pipeline = Pipeline(
            extract=Extract(extract_data,
                         source="./data.txt"),
            steps=[
                Transform(add_value_to_list,
                          2)
            ],
            load=Load(load_data,
                      destination="./transformed_data.txt")
        )
        pipeline.run()

        transformed = extract_data("./transformed_data.txt")
        self.assertEqual(transformed, [3,4,5])


    @classmethod
    def tearDownClass(cls):
       os.system("rm ./transformed_data.txt")


if __name__ == "__main__":
    unittest.main()
