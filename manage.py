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
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def prices_range(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    precos.batch(start_date=start_date, end_date=end_date)


@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def movimentacao_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    movimentos.batch(start_date=start_date, end_date=end_date)


@cli.command()
def pls():
    plfund.run()


@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def pls_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    plfund.batch(start_date=start_date, end_date=end_date)


@cli.command()
def posicao():
    positions.run()


@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def posicao_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    positions.batch(start_date=start_date, end_date=end_date)


@cli.command()
def operations():
    trades_tpe.run()


@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def operations_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    trades_tpe.batch(start_date=start_date, end_date=end_date)


@cli.command()
def carteiras():
    portfolio.run()

@cli.command()
@click.option('--start', default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="Start date YYYY-MM-DD")
@click.option('--end',   default=None, type=click.DateTime(formats=["%Y-%m-%d"]), help="End date YYYY-MM-DD")
def carteiras_batch(start, end):
    start_date = start.date() if start else None
    end_date   = end.date()   if end   else None
    portfolio.batch(start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    cli()
