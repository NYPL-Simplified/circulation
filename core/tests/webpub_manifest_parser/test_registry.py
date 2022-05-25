import pytest
from unittest import TestCase

from core.util.webpub_manifest_parser.core.registry import Registry, RegistryItem


class RegistryTest(TestCase):
    def test_registry_does_not_allow_to_pass_non_registry_item_objects_to_constructor(
        self,
    ):
        # Arrange
        new_registry_item = "dummy"

        # Act, assert
        with pytest.raises(ValueError):
            Registry(new_registry_item)

    def test_registry_allows_to_add_registry_items_through_constructor(self):
        # Arrange
        registry_item_key = "NEW_REGISTRY_ITEM_KEY"
        registry_item = RegistryItem(registry_item_key)

        # Act
        registry = Registry([registry_item])

        # Assert
        assert registry_item_key in registry

    def test_registry_does_not_allow_to_add_non_registry_item_objects(self):
        # Arrange
        registry = Registry()

        # Act, assert
        with pytest.raises(ValueError):
            registry["NEW_REGISTRY_ITEM_KEY"] = "dummy"

    def test_registry_allows_to_add_registry_item_objects(self):
        # Arrange
        registry_item_key = "NEW_REGISTRY_ITEM_KEY"
        registry_item = RegistryItem(registry_item_key)
        registry = Registry()

        # Act
        registry[registry_item_key] = registry_item

        # Assert
        assert registry_item_key in registry

    def test_registry_allows_to_delete_item(self):
        # Arrange
        registry_item_key = "NEW_REGISTRY_ITEM_KEY"
        registry_item = RegistryItem(registry_item_key)
        registry = Registry([registry_item])

        # Act
        del registry[registry_item_key]

        # Assert
        assert registry_item_key not in registry

    def test_registry_returns_correct_number_of_items(self):
        # Arrange
        registry_item_key = "NEW_REGISTRY_ITEM_KEY"
        registry_item = RegistryItem(registry_item_key)
        registry = Registry([registry_item])

        # Act
        result = len(registry)

        # Assert
        assert result == 1
