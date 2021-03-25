# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.
from abc import ABCMeta
from xml.etree import ElementTree  # nosec nosemgrep

from six import add_metaclass


def from_xml_to_obj(xml, obj_type):
    """
    Map a given xml document into a python object.

    The python object you want to map the xml into needs to define a MAPPINGS dictionary which declare how
    to map each tag of the xml doc into the object itself.
    Each entry of the MAPPINGS dictionary is composed as follow:
    - key: name of the xml tag to map
    - value: a dict containing:
        - field: name of the object attribute you want to map the value to
        - transformation: a function that will be called on the value before assigning this to the object attribute.
        - xml_elem_type: how to interpret the tag content. values: {text, xml}, defaults to text
    Default values can be defined in the class __init__ definition.

    :param xml: string containing the xml doc to parse
    :param obj_type: type of the object you want to map the xml into
    :return: an instance of obj_type containing the xml data
    """
    obj = obj_type()
    root = ElementTree.fromstring(xml)  # nosec
    for tag, mapping in obj_type.MAPPINGS.items():
        results = root.findall(tag)
        transformation_func = mapping.get("transformation")
        xml_elem_type = mapping.get("xml_elem_type", "text")
        values = []
        for result in results:
            if xml_elem_type == "xml":
                input = result
            else:
                input = result.text
                if input:
                    input = input.strip()
            values.append(input if transformation_func is None else transformation_func(input))
        if values:
            setattr(obj, mapping["field"], values[0] if len(values) == 1 else values)

    return obj


@add_metaclass(ABCMeta)
class ComparableObject:
    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        attrs = ", ".join(["{key}={value}".format(key=key, value=repr(value)) for key, value in self.__dict__.items()])
        return "{class_name}({attrs})".format(class_name=self.__class__.__name__, attrs=attrs)
