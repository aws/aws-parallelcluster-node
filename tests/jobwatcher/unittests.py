import unittest

from jobwatcher.plugins import utils

instance_properties = {"slots": 8}


class OptimalNodeCountTests(unittest.TestCase):
    def test_empty_lists(self):
        nodes = utils.get_optimal_nodes([], [], instance_properties["slots"])
        expected = 0
        self.assertEqual(nodes, expected)

    def test_each_node_at_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [{"slots": 8}, {"slots": 40}, {"slots": 24}], instance_properties)
        expected = 9
        self.assertEqual(nodes, expected)

    def test_each_node_half_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [{"slots": 4}, {"slots": 20}, {"slots": 12}], instance_properties)
        expected = 5
        self.assertEqual(nodes, expected)

    def test_each_node_one_vcpu_except_max(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [{"slots": 1}, {"slots": 40}, {"slots": 1}], instance_properties)
        expected = 8
        self.assertEqual(nodes, expected)

    def test_each_node_partial_capacity(self):
        nodes = utils.get_optimal_nodes(
            [1, 5, 3, 2], [{"slots": 6}, {"slots": 35}, {"slots": 1}, {"slots": 1}], instance_properties
        )
        expected = 6
        self.assertEqual(nodes, expected)


if __name__ == "__main__":
    unittest.main()
