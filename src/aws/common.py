# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "LICENSE.txt" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and
# limitations under the License.

import functools
import logging
import time
from enum import Enum

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, ParamValidationError

LOGGER = logging.getLogger(__name__)


class AWSClientError(Exception):
    """Error during execution of some AWS calls."""

    class ErrorCode(Enum):
        """Error codes for AWS ClientError."""

        VALIDATION_ERROR = "ValidationError"
        REQUEST_LIMIT_EXCEEDED = "RequestLimitExceeded"
        THROTTLING_EXCEPTION = "ThrottlingException"
        CONDITIONAL_CHECK_FAILED_EXCEPTION = "ConditionalCheckFailedException"

        @classmethod
        def throttling_error_codes(cls):
            """Return a set of error codes returned when service rate limits are exceeded."""
            return {cls.REQUEST_LIMIT_EXCEEDED.value, cls.THROTTLING_EXCEPTION.value}

    def __init__(self, function_name: str, message: str, error_code: str = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.function_name = function_name


class LimitExceededError(AWSClientError):
    """Error caused by exceeding the limits of a downstream AWS service."""

    def __init__(self, function_name: str, message: str, error_code: str = None):
        super().__init__(function_name=function_name, message=message, error_code=error_code)


class BadRequestError(AWSClientError):
    """Error caused by a problem in the request."""

    def __init__(self, function_name: str, message: str, error_code: str = None):
        super().__init__(function_name=function_name, message=message, error_code=error_code)


class AWSExceptionHandler:
    """AWS Exception handler."""

    @staticmethod
    def handle_client_exception(func):
        """Handle Boto3 errors, can be used as a decorator."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ParamValidationError as validation_error:
                error = BadRequestError(
                    func.__name__,
                    "Error validating parameter. Failed with exception: {0}".format(str(validation_error)),
                )
            except BotoCoreError as e:
                error = AWSClientError(func.__name__, str(e))
            except ClientError as e:
                # add request id
                message = e.response["Error"]["Message"]
                error_code = e.response["Error"]["Code"]

                if error_code in AWSClientError.ErrorCode.throttling_error_codes():
                    error = LimitExceededError(func.__name__, message, error_code)
                elif error_code == AWSClientError.ErrorCode.VALIDATION_ERROR:
                    error = BadRequestError(func.__name__, message, error_code)
                else:
                    error = AWSClientError(func.__name__, message, error_code)
            LOGGER.error("Encountered error when performing boto3 call in %s: %s", error.function_name, error.message)
            raise error

        return wrapper

    @staticmethod
    def retry_on_boto3_throttling(func):
        """Retry boto3 calls on throttling, can be used as a decorator."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            while True:
                try:
                    return func(*args, **kwargs)
                except ClientError as e:
                    if e.response["Error"]["Code"] != "Throttling":
                        raise
                    LOGGER.debug("Throttling when calling %s function. Will retry in %d seconds.", func.__name__, 5)
                    time.sleep(5)

        return wrapper


def _log_boto3_calls(params, **kwargs):
    service = kwargs["event_name"].split(".")[-2]
    operation = kwargs["event_name"].split(".")[-1]
    region = kwargs["context"].get("client_region", boto3.session.Session().region_name)
    LOGGER.info(
        "Executing boto3 call: region=%s, service=%s, operation=%s, params=%s", region, service, operation, params
    )


class Boto3Client:
    """Boto3 client Class."""

    def __init__(self, client_name: str, config: Config, region: str = None):
        region = region if region else get_region()
        self._client = boto3.client(client_name, region_name=region, config=config if config else None)
        self._client.meta.events.register("provide-client-params.*.*", _log_boto3_calls)

    def _paginate_results(self, method, **kwargs):
        """
        Return a generator for a boto3 call, this allows pagination over an arbitrary number of responses.

        :param method: boto3 method
        :param kwargs: arguments to method
        :return: generator with boto3 results
        """
        paginator = self._client.get_paginator(method.__name__)
        for page in paginator.paginate(**kwargs).result_key_iters():
            for result in page:
                yield result


class Boto3Resource:
    """Boto3 resource Class."""

    def __init__(self, resource_name: str):
        self._resource = boto3.resource(resource_name)
        self._resource.meta.client.meta.events.register("provide-client-params.*.*", _log_boto3_calls)


def get_region():
    """Get region used internally for all the AWS calls."""
    region = boto3.session.Session().region_name
    if region is None:
        raise AWSClientError("get_region", "AWS region not configured")
    return region
