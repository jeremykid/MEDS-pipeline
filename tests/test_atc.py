import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src', 'atc-ndc-bidirectional-conversion', 'mappings'))

from lookup_code import lookup_code

def test_lookup_code():
    # test ATC code
    result = lookup_code("C10AA07")
    print(result)
    assert "ATC Code:" in result or "not found" in result
    
def test_lookup_ndc():
    # test NDC code
    result = lookup_code("47335-0985-60")
    print(result)
    assert "NDC Code:" in result or "not found" in result

if __name__ == "__main__":
    test_lookup_code()
    test_lookup_ndc()