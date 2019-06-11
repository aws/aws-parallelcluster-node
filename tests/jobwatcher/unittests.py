import unittest

from jobwatcher.plugins import utils

instance_properties = {"slots": 8}


class optimal_node_count_tests(unittest.TestCase):
    def test_empty_lists(self):
        nodes = utils.get_optimal_nodes([], [], instance_properties)
        expected = 0
        self.assertEqual(nodes, expected)

    def test_each_node_at_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [8, 40, 24], instance_properties)
        expected = 9
        self.assertEqual(nodes, expected)

    def test_slots_requested_greater_than_available(self):
        nodes = utils.get_optimal_nodes([1], [9], instance_properties)
        expected = 2
        self.assertEqual(nodes, expected)

    def test_each_node_half_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [4, 20, 12], instance_properties)
        expected = 5
        self.assertEqual(nodes, expected)

    def test_each_node_one_vcpu_except_max(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [1, 40, 1], instance_properties)
        expected = 8
        self.assertEqual(nodes, expected)

    def test_each_node_partial_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3, 2], [6, 35, 1, 1], instance_properties)
        expected = 6
        self.assertEqual(nodes, expected)


if __name__ == "__main__":
    unittest.main()
