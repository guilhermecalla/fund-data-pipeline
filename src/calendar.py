# based on: https://github.com/quantopian/trading_calendars/blob/master/trading_calendars/exchange_calendar_bvmf.py

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta

from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    Day,
    Easter,
    GoodFriday,
    Holiday,
    previous_friday,
)
from pandas.tseries.offsets import (
    CustomBusinessMonthEnd,
    CustomBusinessMonthBegin,
    CustomBusinessDay,
)


class HolidayCalendar(AbstractHolidayCalendar):
    def __init__(self, rules):
        super(HolidayCalendar, self).__init__(rules=rules)


ConfUniversal = Holiday(
    "Dia da Confraternizacao Universal",
    month=1,
    day=1,
)

CarnavalSegunda = Holiday(
    "Carnaval Segunda", month=1, day=1, offset=[Easter(), Day(-48)]
)

CarnavalTerca = Holiday("Carnaval Terca", month=1, day=1, offset=[Easter(), Day(-47)])
QuartaCinzas = Holiday("Quarta Cinzas", month=1, day=1, offset=[Easter(), Day(-46)])


SextaPaixao = GoodFriday
CorpusChristi = Holiday(
    "Corpus Christi",
    month=1,
    day=1,
    offset=[Easter(), Day(60)],
)
# Tiradentes Memorial
Tiradentes = Holiday(
    "Tiradentes",
    month=4,
    day=21,
)
# Labor Day
DiaTrabalho = Holiday(
    "Dia Trabalho",
    month=5,
    day=1,
)

Constitucionalista_prepandemic = Holiday(
    "Constitucionalista pre-pandemia",
    month=7,
    day=9,
    start_date="1998-01-01",
    end_date="2020-01-01",
)

Independencia = Holiday(
    "Independencia",
    month=9,
    day=7,
)
# Our Lady of Aparecida
Aparecida = Holiday(
    "Nossa Senhora de Aparecida",
    month=10,
    day=12,
)
# All Souls' Day
Finados = Holiday(
    "Dia dos Finados",
    month=11,
    day=2,
)
# Proclamation of the Republic
ProclamacaoRepublica = Holiday(
    "Proclamacao da Republica",
    month=11,
    day=15,
)
# Day of Black Awareness

# Christmas Eve
VesperaNatal = Holiday(
    "Vespera Natal",
    month=12,
    day=24,
)
# Christmas
Natal = Holiday(
    "Natal",
    month=12,
    day=25,
)
# New Year's Eve

CopaDoMundo2014 = Holiday("Copa Do Mundo 2014", month=6, day=12, year=2014)


class TarponCalendar:

    def __init__(self, include_new_years_eve_holiday=True) -> None:
        _holiday_list = [
            ConfUniversal,
            CarnavalSegunda,
            CarnavalTerca,
            SextaPaixao,
            CorpusChristi,
            Tiradentes,
            DiaTrabalho,
            Constitucionalista_prepandemic,
            Independencia,
            CopaDoMundo2014,
            Aparecida,
            Finados,
            ProclamacaoRepublica,
            VesperaNatal,
            Natal,
        ]

        holiday_calendar = HolidayCalendar(_holiday_list)

        self.custom_calendar_month_end = CustomBusinessMonthEnd(
            calendar=holiday_calendar
        )
        self.custom_calendar_month_begin = CustomBusinessMonthBegin(
            calendar=holiday_calendar
        )
        self.custom_calendar_day = CustomBusinessDay(calendar=holiday_calendar)

    def get_previous_trading_day(self, date: datetime.date):
        return date - self.custom_calendar_day

    def get_last_trading_day_of_month(self, date: datetime.date):
        """Function that return the last day of trading given a date"""
        return date + self.custom_calendar_month_end

    def get_first_trading_day_of_month(self, date: datetime.date):
        """Function that return the first day of trading given a date"""
        return date - self.custom_calendar_month_begin

    def get_last_trading_day_of_previous_month(self, date: datetime.date):
        date = datetime.date(date.year, date.month, 1) - relativedelta(months=1)

        return self.get_last_trading_day_of_month(date)

    def get_last_trading_day_of_previous_year(self, date: datetime.date):
        """
        Função que retorna o último dia útil do ano anterior à data fornecida.
        """
        date = datetime.date(date.year - 1, 12, 1)

        # Usa a função get_last_trading_day_of_month para garantir que é um dia útil
        return self.get_last_trading_day_of_month(date)

    def get_last_trading_day_of_ltm(self, date: datetime.date):
        date = datetime.date(date.year, date.month, 1) - relativedelta(months=12)

        return self.get_last_trading_day_of_month(date)

    def get_last_trading_day_of_last_six_month(self, date: datetime.date):
        date = datetime.date(date.year, date.month, 1) - relativedelta(months=6)

        return self.get_last_trading_day_of_month(date)

    def get_last_trading_day_of_24m(self, date: datetime.date):
        date = datetime.date(date.year, date.month, 1) - relativedelta(months=24)

        return self.get_last_trading_day_of_month(date)

    def get_last_trading_day_of_36m(self, date: datetime.date):
        date = datetime.date(date.year, date.month, 1) - relativedelta(months=36)

        return self.get_last_trading_day_of_month(date)

    def get_last_trading_day_of_48m(self, date: datetime.date):
        date = datetime.date(date.year, date.month, 1) - relativedelta(months=48)

        return self.get_last_trading_day_of_month(date)

    def get_last_trading_day_of_60m(self, date: datetime.date):
        date = datetime.date(date.year, date.month, 1) - relativedelta(months=60)

        return self.get_last_trading_day_of_month(date)

    def get_business_days_in_month(self, year, month):
        date_start = datetime.date(year, month, 1)
        date_end = date_start + relativedelta(months=1) - datetime.timedelta(days=1)
        return self.get_business_days_in_range(date_start, date_end)

    def get_business_days_in_range(
        self, start_date: datetime.date, end_date: datetime.date
    ):
        """
        Calcula a quantidade de dias úteis em um intervalo de datas usando pandas.

        Parameters:
        start_date (datetime.date): Data inicial
        end_date (datetime.date): Data final

        Returns:
        int: Número de dias úteis no intervalo
        """

        # Criar um range de datas de negociação usando o calendário personalizado
        business_days = pd.date_range(
            start=start_date, end=end_date, freq=self.custom_calendar_day
        )

        return business_days
