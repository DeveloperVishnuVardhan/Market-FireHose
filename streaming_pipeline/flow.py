import datetime
from pathlib import Path
from typing import List, Optional
from pydantic import parse_obj_as

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.inputs import Source

from streaming_pipeline import mocked
from streaming_pipeline.models import NewsArticle


def build(
        is_batch: bool,
        from_datetime: Optional[datetime.date],
        to_datetime: Optional[datetime.date],
        model_cache_dir: Optional[Path] = None,
        debug: bool = False,
) -> Dataflow:
    """Builds a dataflow pipeline for processing news articles.

    Args:
        is_batch (bool): whether the pipeline is processing batch or stream of articles.
        from_datetime (Optional[datetime.date]): if start the start time of the batch.
        to_datetime (Optional[datetime.date]): the end time of the batch.
        model_cache_dir (Optional[Path], optional): _description_. The directory to cache the embedding model.
        debgug (bool, optional): _description_. whether to enable debug mode.

    Returns:
        Dataflow: The output dataflow pipeline.
    """
    is_input_mocked = debug is True and is_batch is False
    flow = Dataflow("Collecting vectorized data")
    stream = op.input(
        "input",
        flow,
        _build_input(
            is_batch, from_datetime, to_datetime, is_input_mocked=is_input_mocked
        )
    )

    # Add step_id to flat_map and return the stream
    stream = op.flat_map(
        "parse_messages",  # step_id is required
        stream,
        lambda messages: parse_obj_as(List[NewsArticle], messages)
    )

    if debug:
        stream = op.inspect("debug_output", stream)

    return flow  # Return the flow


def _build_input(
        is_batch: bool = False,
        from_datetime: Optional[datetime.datetime] = None,
        to_datetime: Optional[datetime.datetime] = None,
        is_input_mocked: bool = False,
) -> Source:
    if is_input_mocked is True:
        return TestingSource(mocked.financial_news)
    if is_batch:
        assert (
            from_datetime is not None and to_datetime is not None
        ), "from_datetime and to_datetime must be provided when is_batch is True"
        # To be implemented.
        return TestingSource([])
    else:
        # To be implemented.
        return TestingSource([])
