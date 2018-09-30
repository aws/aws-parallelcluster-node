import utils
import unittest

instance_properties = {'slots': 8}


class optimal_node_count_tests(unittest.TestCase):
    def test_empty_lists(self):
        nodes = utils.get_optimal_nodes([], [], instance_properties)
        expected = 0
        self.assertEqual(nodes, expected, "test_empty_lists failed. Got %s; Expected: %s" % (nodes, expected))

    def test_each_node_at_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [8, 40, 24], instance_properties)
        expected = 9
        self.assertEqual(nodes, expected, "test_exact_fit failed. Got %s; Expected: %s" % (nodes, expected))

    def test_only_vcpus(self):
        nodes = utils.get_optimal_nodes([1], [27], instance_properties)
        expected = 4
        self.assertEqual(nodes, expected, "test_exact_fit failed. Got %s; Expected: %s" % (nodes, expected))

    def test_each_node_half_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [4, 20, 12], instance_properties)
        expected = 5
        self.assertEqual(nodes, expected, "test_exact_fit failed: Got %s; Expected: %s" % (nodes, expected))

    def test_each_node_one_vcpu_except_max(self):
        nodes = utils.get_optimal_nodes([1, 5, 3], [1, 40, 1], instance_properties)
        expected = 8
        self.assertEqual(nodes, expected, "test_each_node_one_vcpu_except_max failed: Got %s; Expected: %s" % (nodes, expected))

    def test_each_node_partial_capacity(self):
        nodes = utils.get_optimal_nodes([1, 5, 3, 2], [6, 35, 1, 1], instance_properties)
        expected = 6
        self.assertEqual(nodes, expected, "test_each_node_partial_capacity failed: Got %s; Expected: %s" % (nodes, expected))


if __name__ == '__main__':
    unittest.main()
