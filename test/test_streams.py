import pendulum
from tap_zendesk_chat.streams import break_into_intervals


def test_intervals():
    days = 30
    now = pendulum.parse("2018-02-14T10:30:20")
    broken = break_into_intervals(days, "2018-01-02T18:14:33", now)
    as_strs = [(x.isoformat(), y.isoformat()) for x, y in broken]
    assert as_strs == [
        ("2018-01-02T18:14:33+00:00", "2018-02-01T18:14:33+00:00"),
        ("2018-02-01T18:14:33+00:00", "2018-02-14T10:30:20+00:00"),
    ]

