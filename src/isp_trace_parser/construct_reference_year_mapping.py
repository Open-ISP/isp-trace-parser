def construct_reference_year_mapping(start_year, end_year, reference_years):
    """Constructs a dict mapping a sequence of modelling years to a cycle of reference years.

    Examples:

    >>> construct_reference_year_mapping(
    ... start_year=2030,
    ... end_year=2035,
    ... reference_years=[2011, 2013, 2018],
    ... )
    {2030: 2011, 2031: 2013, 2032: 2018, 2033: 2011, 2034: 2013, 2035: 2018}

    Args:
        start_year: int, first year in sequence of modelling years
        end_year: int, last year in sequence of modelling years
        reference_years: list[int], list of reference years to cycle through when constructing the mapping.
    """
    years = range(start_year, end_year + 1)
    mapping_length = len(years)
    length_of_cycle = len(reference_years)
    if mapping_length <= length_of_cycle:
        reference_years = reference_years[:mapping_length]
    else:
        full_reference_year_cycles = mapping_length // length_of_cycle
        partial_cycle_length = mapping_length % length_of_cycle
        reference_years = (
            reference_years * full_reference_year_cycles
        ) + reference_years[:partial_cycle_length]
    return dict(zip(years, reference_years))