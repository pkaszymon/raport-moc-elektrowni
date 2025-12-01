"""
Tests for PSE API module.

Tests the detect_new_labels function which identifies new power plants and
resource codes that are not present in the application constants.
"""

import pytest
from pse_api import detect_new_labels, POWER_PLANT_TO_RESOURCES, ALL_RESOURCE_CODES


class TestDetectNewLabels:
    """Test suite for the detect_new_labels function."""
    
    def test_empty_data(self):
        """Test with empty data - should return no new labels."""
        result = detect_new_labels([])
        
        assert result['has_new_labels'] is False
        assert result['new_power_plants'] == []
        assert result['new_resource_codes'] == []
        assert result['new_mapping'] == {}
    
    def test_known_labels_only(self):
        """Test with only known power plants and resource codes - should return no new labels."""
        test_data = [
            {'power_plant': 'Bełchatów', 'resource_code': 'BEL 2-02'},
            {'power_plant': 'Turów', 'resource_code': 'TUR 1-01'},
            {'power_plant': 'Opole', 'resource_code': 'OPL 1-01'},
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is False
        assert result['new_power_plants'] == []
        assert result['new_resource_codes'] == []
        assert result['new_mapping'] == {}
    
    def test_new_power_plant(self):
        """Test detection of a completely new power plant."""
        test_data = [
            {'power_plant': 'Bełchatów', 'resource_code': 'BEL 2-02'},
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-01'},
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        assert 'Nowa Elektrownia' in result['new_power_plants']
        assert 'NEW 1-01' in result['new_resource_codes']
        assert 'Nowa Elektrownia' in result['new_mapping']
        assert 'NEW 1-01' in result['new_mapping']['Nowa Elektrownia']
    
    def test_new_power_plant_with_multiple_codes(self):
        """Test detection of a new power plant with multiple resource codes."""
        test_data = [
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-01'},
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-02'},
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-03'},
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        assert len(result['new_power_plants']) == 1
        assert result['new_power_plants'][0] == 'Nowa Elektrownia'
        assert len(result['new_resource_codes']) == 3
        assert 'NEW 1-01' in result['new_resource_codes']
        assert 'NEW 1-02' in result['new_resource_codes']
        assert 'NEW 1-03' in result['new_resource_codes']
        assert len(result['new_mapping']['Nowa Elektrownia']) == 3
    
    def test_existing_plant_with_new_resource_code(self):
        """Test detection of new resource codes for an existing power plant."""
        test_data = [
            {'power_plant': 'Bełchatów', 'resource_code': 'BEL 2-02'},  # Known
            {'power_plant': 'Bełchatów', 'resource_code': 'BEL 9-99'},  # New
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        assert result['new_power_plants'] == []  # Plant is known
        assert 'BEL 9-99' in result['new_resource_codes']
        assert 'Bełchatów' in result['new_mapping']
        assert 'BEL 9-99' in result['new_mapping']['Bełchatów']
        assert 'BEL 2-02' not in result['new_mapping']['Bełchatów']  # Known code should not be in mapping
    
    def test_multiple_existing_plants_with_new_codes(self):
        """Test detection when multiple existing plants have new resource codes."""
        test_data = [
            {'power_plant': 'Bełchatów', 'resource_code': 'BEL 9-99'},
            {'power_plant': 'Turów', 'resource_code': 'TUR 9-99'},
            {'power_plant': 'Opole', 'resource_code': 'OPL 9-99'},
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        assert result['new_power_plants'] == []
        assert len(result['new_resource_codes']) == 3
        assert len(result['new_mapping']) == 3
        assert 'Bełchatów' in result['new_mapping']
        assert 'Turów' in result['new_mapping']
        assert 'Opole' in result['new_mapping']
    
    def test_mixed_new_plants_and_new_codes(self):
        """Test detection with both new power plants and new codes for existing plants."""
        test_data = [
            # Existing plant with new code
            {'power_plant': 'Bełchatów', 'resource_code': 'BEL 9-99'},
            # New plant with codes
            {'power_plant': 'Nowa Elektrownia A', 'resource_code': 'NEA 1-01'},
            {'power_plant': 'Nowa Elektrownia A', 'resource_code': 'NEA 1-02'},
            # Another new plant
            {'power_plant': 'Nowa Elektrownia B', 'resource_code': 'NEB 1-01'},
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        assert len(result['new_power_plants']) == 2
        assert 'Nowa Elektrownia A' in result['new_power_plants']
        assert 'Nowa Elektrownia B' in result['new_power_plants']
        assert len(result['new_resource_codes']) == 4
        # Check mapping includes both new plants and existing plants with new codes
        assert 'Bełchatów' in result['new_mapping']
        assert 'Nowa Elektrownia A' in result['new_mapping']
        assert 'Nowa Elektrownia B' in result['new_mapping']
    
    def test_duplicate_records(self):
        """Test that duplicate records are handled correctly."""
        test_data = [
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-01'},
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-01'},  # Duplicate
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-01'},  # Duplicate
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        assert len(result['new_power_plants']) == 1
        assert len(result['new_resource_codes']) == 1
        assert len(result['new_mapping']['Nowa Elektrownia']) == 1
    
    def test_records_with_missing_fields(self):
        """Test handling of records with missing power_plant or resource_code fields."""
        test_data = [
            {'power_plant': 'Nowa Elektrownia', 'resource_code': 'NEW 1-01'},
            {'power_plant': None, 'resource_code': 'NEW 1-02'},  # Missing plant
            {'power_plant': 'Inna Elektrownia', 'resource_code': None},  # Missing code
            {'other_field': 'value'},  # Missing both
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        # Should only detect the complete record
        assert 'Nowa Elektrownia' in result['new_power_plants']
        assert 'NEW 1-01' in result['new_resource_codes']
    
    def test_sorted_output(self):
        """Test that output lists are sorted alphabetically."""
        test_data = [
            {'power_plant': 'Zebra Elektrownia', 'resource_code': 'ZEB 1-01'},
            {'power_plant': 'Alpha Elektrownia', 'resource_code': 'ALP 1-01'},
            {'power_plant': 'Beta Elektrownia', 'resource_code': 'BET 1-01'},
        ]
        
        result = detect_new_labels(test_data)
        
        # Check that power plants are sorted
        assert result['new_power_plants'] == ['Alpha Elektrownia', 'Beta Elektrownia', 'Zebra Elektrownia']
        # Check that resource codes are sorted
        assert result['new_resource_codes'] == ['ALP 1-01', 'BET 1-01', 'ZEB 1-01']
        # Check that mapping values are sorted
        assert result['new_mapping']['Zebra Elektrownia'] == ['ZEB 1-01']
    
    def test_only_new_resource_code_without_plant(self):
        """Test detection of new resource code when power plant is None."""
        test_data = [
            {'power_plant': None, 'resource_code': 'ORPHAN 1-01'},
        ]
        
        result = detect_new_labels(test_data)
        
        assert result['has_new_labels'] is True
        assert result['new_power_plants'] == []
        assert 'ORPHAN 1-01' in result['new_resource_codes']
        # Should not appear in mapping since there's no plant
        assert result['new_mapping'] == {}
    
    def test_constants_are_used(self):
        """Test that the function correctly uses POWER_PLANT_TO_RESOURCES and ALL_RESOURCE_CODES constants."""
        # Get a known plant and code from constants
        known_plant = list(POWER_PLANT_TO_RESOURCES.keys())[0]
        known_code = POWER_PLANT_TO_RESOURCES[known_plant][0]
        
        test_data = [
            {'power_plant': known_plant, 'resource_code': known_code},
        ]
        
        result = detect_new_labels(test_data)
        
        # Should not detect known plant/code as new
        assert result['has_new_labels'] is False
        assert known_plant not in result['new_power_plants']
        assert known_code not in result['new_resource_codes']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
