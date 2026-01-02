import click
from src import movimentos
from src import precos
from src import plfund
from src import positions
from src import trades_tpe
from src import portfolio


@click.group()
def cli():
    pass


@cli.command()
def movimentacao():
    movimentos.run()


@cli.command()
def prices():
    precos.run()


@cli.command()
def prices_range():
    precos.batch()


@cli.command()
def movimentacao_batch():
    movimentos.batch()


@cli.command()
def pls():
    plfund.run()


@cli.command()
def pls_batch():
    plfund.batch()


@cli.command()
def posicao():
    positions.run()


@cli.command()
def posicao_batch():
    positions.batch()


@cli.command()
def operations():
    trades_tpe.run()


@cli.command()
def operations_batch():
    trades_tpe.batch()


@cli.command()
def carteiras():
    portfolio.run()

@cli.command()
def carteiras_batch():
    portfolio.batch()


if __name__ == "__main__":
    cli()
