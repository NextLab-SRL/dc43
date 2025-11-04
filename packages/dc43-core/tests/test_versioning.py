from dc43_core.versioning import SemVer


def test_semver_parse_and_bump():
    ver = SemVer.parse("1.2.3-draft+meta")
    assert str(ver) == "1.2.3-draft+meta"
    assert SemVer.parse(str(ver.bump("major"))) == SemVer(2, 0, 0)
    assert SemVer.parse(str(ver.bump("minor"))) == SemVer(1, 3, 0)
    assert SemVer.parse(str(ver.bump("patch"))) == SemVer(1, 2, 4)
