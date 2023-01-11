from cachelib import DynamoDbCache as CachelibDynamoDbCache

from flask_caching.backends.base import BaseCache


class DynamoDbCache(BaseCache, CachelibDynamoDbCache):
    """Uses AWS DynamoDB as a cache backend.
    :param table_name: The name of the DynamoDB table to use
    :param default_timeout: Set the timeout in seconds after which cache entries
                            expire
    :param key_field: The name of the hash_key attribute in the DynamoDb
                      table. This must be a string attribute.
    :param expiration_time_field: The name of the table attribute to store the
                                  expiration time in.  This will be an int
                                  attribute. The timestamp will be stored as
                                  seconds past the epoch.  If you configure
                                  this as the TTL field, then DynamoDB will
                                  automatically delete expired entries.
    """

    def __init__(
        self,
        table_name="python-cache",
        default_timeout=300,
        key_field="cache_key",
        expiration_time_field="expiration_time",
        **kwargs
    ):
        BaseCache.__init__(self, default_timeout=default_timeout)
        CachelibDynamoDbCache.__init__(
            self,
            table_name=table_name,
            key_field=key_field,
            expiration_time_field=expiration_time_field,
            **kwargs
        )

    @classmethod
    def factory(cls, app, config, args, kwargs):
        kwargs.update(
            dict(
                table_name=config.get("DYNAMODB_CACHE_TABLE", "python-cache"),
                key_field=config.get("DYNAMODB_CACHE_KEY_FIELD", "key"),
                expiration_time_field=config.get(
                    "DYNAMODB_CACHE_TTL_FIELD", "expire_at"
                ),
                region_name=config.get("DYNAMODB_CACHE_REGION", "us-east-1"),
            )
        )
        if config.get("DYNAMODB_ENDPOINT_URL"):
            kwargs["endpoint_url"] = config["DYNAMODB_ENDPOINT_URL"]

        new_class = cls(*args, **kwargs)

        return new_class
