from streaming_pipeline import mocked
from streaming_pipeline.flow import build
from bytewax.testing import run_main
import datetime

# Use your existing build function
flow = build(
    is_batch=True,
    from_datetime=datetime.datetime.now(),
    to_datetime=datetime.datetime.now(),
    debug=True
)

# Since debug=True, it will use TestingSource with mocked.financial_news
run_main(flow)
