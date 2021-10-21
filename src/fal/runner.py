"""Run forecasts with FAL."""
import click


@click.command()
@click.argument("model")
def run(model):
    """Run forecast."""
    print(f"Forecast will run on {model}")
