from typing import Iterable

from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner


def mean(xs: Iterable[float], n: int) -> float:
    return sum(xs) / n

def squares(xs: Iterable[float]) -> Iterable[float]:
    return [x**2 for x in xs]

def variance(mean_squares: float, mean: float) -> float:
    return mean_squares - mean**2

mean_node = node(func=mean, inputs=["xs", "n"], outputs="mean", name="mean")

variance_pipeline = pipeline(
        node(func=len, inputs=["xs"], outputs="n"),
        node(func=mean, inputs=["xs", "n"], outputs="mean"),
        node(func=squares, inputs="xs", outputs="squares"),
        node(func=mean, inputs=["squares", "n"], outputs="mean_squares"),
        node(func=variance, inputs=["mean_squares", "mean"], output="variance")
        
    )

runner = SequentialRunner()

calculated_variance = runner.run(
    pipeline=variance_pipeline,
    catalog=DataCatalog(
        data_sets={'xs': MemoryDataSet(data=[1,2,3])}
    ),
)

print(calculated_variance)
