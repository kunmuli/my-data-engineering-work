import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def camel_to_snake(name):
    # Replace lowercase-uppercase transitions with underscores
    name = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', name)
    return name.lower()

@transformer
def transform(data, *args, **kwargs):

    #

    # clean column names, make all lowercase and convert to snake_case
    data.columns = data.columns.map(camel_to_snake)

    # create new column of date dtype for 'lpep_pickup_date' from 'lpep_pickup_datetime'
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    # drop records of rides with no passengers
    print("Rows with zero passengers:", data['passenger_count'].isin([0]).sum())
    data = data[data['passenger_count'] > 0]

    # drop records of rides with 0 trip_distance
    print("Rows with 0 trip_distance: ",data['trip_distance'].isin([0]).sum())
    data = data[data['trip_distance'] > 0]

    # print rows left after filtering
    print("Rows without zero passengers and 0 trip_distance: ", data.shape)

    # print existing values of VendorId in the dataset
    print("Values of VendorId in the dataset:\n", data.vendor_id.value_counts())

    return data

    """
        Add three assertions:
        - vendor_id is one of the existing values in the column (currently)
        - passenger_count is greater than 0
        - trip_distance is greater than 0
        """
@test
def test_col_vendor_id(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, 'vendor_id not found in column names.'

@test
def test_passenger_count(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passengers'

@test
def test_trip_distance(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with 0 trip_distance'