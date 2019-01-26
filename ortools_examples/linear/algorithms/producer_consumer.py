from datetime import datetime, timedelta
from dateutil.rrule import rrule, MINUTELY

from ortools.linear_solver import pywraplp

"""
Definitions
-----------
Consumer: 
    Consumer which requires an amount of units of something.
Company: 
    Contains one or more buildings.
Building: 
    Contains one or more producers
Producer: 
    Producer that creates units for the consumer. Has a max_supply constraint.
Delivery: 
    Consumer connected to a producer. Each consumer connects only to a single producer
    The producer to use is decided previously by the user
    depending on the consumer features.
Time block: 
    Time is divided into blocks where supplied units and unit
    price are constant. E.g. 15 minutes.
Schedule:
    A list of delivery-time block-units charged 

Goal
----
Calculate the optimum schedule so we minimize unit cost and this 
constraints are met:
    All consumer receive the required amount of units
    The delivery of the consumers is done before a specific departure date
    Physical constraints are met, no more units than the maximum allowed 
        is supplied. That applies to producers, buildings and the whole company.
"""


class Consumer:
    def __init__(self, id, required_units, departure_date):
        self.id = id
        self.required_units = required_units
        self.departure_date = departure_date

    def __hash__(self):
        return hash(self.id)


class Company:
    def __init__(self, id, max_supply=None):
        self.id = id
        self.max_supply = max_supply
        self.buildings = []

    def add_building(self, building):
        self.buildings.append(building)


class Building:
    def __init__(self, id, max_supply=None, company=None):
        self.id = id
        self.max_supply = max_supply
        self.company = company
        self.producers = []
        if company:
            company.add_building(self)

    def add_producer(self, producer):
        self.producers.append(producer)


class Producer:
    def __init__(self, id, max_supply, building):
        self.id = id
        self.max_supply = max_supply
        self.building = building
        if building:
            building.add_producer(self)

    def __hash__(self):
        return hash(self.id)


class Delivery:
    def __init__(self, consumer, producer):
        self.consumer = consumer
        self.producer = producer
        self.id = f"c-{self.consumer.id}, p-{self.producer.id}"

    def __hash__(self):
        return hash((self.consumer.id, self.producer.id))

    def __str__(self):
        return f"Delivery({self.id})"


class UnitPrice:
    def __init__(self, date_from, date_until, block_number, price):
        self.date_from = date_from
        self.date_until = date_until
        self.block_number = block_number
        self.price = price


class TimeBlock:
    def __init__(self, number, date_from, date_until, price=None):
        self.number = number
        self.date_from = date_from
        self.date_until = date_until
        self.price = price

    @staticmethod
    def ceil_dt(dt, delta):
        return dt + (datetime.min - dt) % delta

    @staticmethod
    def period_to_blocks(blocks_per_hour=4,
                         hours=24,
                         initial_date=datetime.now(),
                         unit_prices=None):
        assert blocks_per_hour > 0
        assert hours > 0
        total_blocks = blocks_per_hour * hours

        minutes_per_block = int(60 / blocks_per_hour)
        start_date = TimeBlock.ceil_dt(
            initial_date,
            timedelta(minutes=minutes_per_block),
        )
        times = list(rrule(MINUTELY,
                           interval=minutes_per_block,
                           dtstart=start_date,
                           count=total_blocks))

        def get_price_for_date_or_number(date, number):
            for energy_price in unit_prices or []:
                if energy_price.block_number == number:
                    return energy_price.price
                elif (energy_price.date_from and energy_price.date_until and
                      energy_price.date_from <= date < energy_price.date_until):
                    return energy_price.price
            return 0

        blocks = {str(number): TimeBlock(
            number=number,
            date_from=time,
            date_until=time + timedelta(minutes=minutes_per_block),
            price=get_price_for_date_or_number(time, number),
        ) for number, time in enumerate(times)}

        return blocks

    def __str__(self):
        return (f"Block {self.number}: {self.date_from}"
                f" to {self.date_until} -> {self.price}€")


class Schedule:
    def __init__(self, name, status, algorithm):
        # build a list of
        #   time block
        #   producer
        #   consumer
        #   amount of units supplied
        # total price for the whole schedule
        self.algorithm = algorithm
        solver = algorithm.solver
        variables = algorithm.variables
        alg_deliveries = algorithm.deliveries
        self.name = name
        self.total_price = 0.0
        self.deliveries = []
        self.solved = status == solver.OPTIMAL or status == solver.FEASIBLE
        if status == solver.OPTIMAL:
            self.result = "Optimal solution found"
        elif status == solver.FEASIBLE:
            self.result = "A potentially suboptimal solution was found."
        else:
            self.result = "The solver could not solve the problem."

        if self.solved:
            for delivery_id, delivery_vars in variables.items():
                delivery = alg_deliveries.get(delivery_id)
                producer = delivery.producer
                consumer = delivery.consumer
                for time_block, variable in delivery_vars.items():
                    value = variable.solution_value()
                    if value > 0:
                        block = self.algorithm.time_blocks.get(str(time_block))
                        self.total_price += value * block.price
                        self.deliveries.append(
                            {
                                "time_block": time_block,
                                "producer": producer.id,
                                "consumer": consumer.id,
                                "units_to_supply": value,
                                "unit_price": block.price,
                            }
                        )

    def print(self):
        self.algorithm.print()
        print("---------------------")
        print(f"RESULT FOR PROBLEM: {self.result}")
        if not self.solved:
            return
        print(f"Total price: {self.total_price}")
        print("Schedule")
        print(
            f"  Block              From             Until  Producer           Consumer  Energy to supply     Price     Total")
        for delivery in sorted(self.deliveries,
                              key=lambda k: k['time_block']):
            block = self.algorithm.time_blocks.get(
                str(delivery.get("time_block")))
            date_from = "".rjust(19)
            if block.date_from:
                date_from = block.date_from.strftime('%Y-%m-%d %H:%M').rjust(16)
            date_until = "".rjust(19)
            if block.date_until:
                date_until = block.date_until.strftime('%Y-%m-%d %H:%M').rjust(
                    16)

            print(f"  {str(delivery.get('time_block')).rjust(5)}"
                  f"  {date_from}"
                  f"  {date_until}"
                  f"  {delivery.get('producer').rjust(14)}"
                  f"  {delivery.get('consumer').rjust(11)}"
                  f"  {str(delivery.get('units_to_supply')).rjust(16)}"
                  f"  {str(block.price).rjust(8)}"
                  f"  {str(block.price * delivery.get('units_to_supply')).rjust(8)}")


class ProducerConsumerAlgorithm:
    def __init__(self, name=None):
        self.initial_date = datetime.now()
        self.blocks_per_hour = 4
        self.hours = 24
        self.time_blocks = {}
        self.unit_prices = []
        self.deliveries = {}
        self.company = None
        self.name = name
        self.variables = None
        self.solver = None

    def with_initial_date(self, initial_date):
        self.initial_date = initial_date
        return self

    def with_hours(self, hours):
        self.hours = hours
        return self

    def with_blocks_per_hour(self, blocks_per_hour):
        self.blocks_per_hour = blocks_per_hour
        return self

    def with_unit_price(self,
                        date_from=None,
                        date_until=None,
                        block_number=None,
                        price=0):
        assert block_number is not None or (
            date_from and date_until), 'Pass block # or dates'
        self.unit_prices.append(
            UnitPrice(
                date_from=date_from,
                date_until=date_until,
                block_number=block_number,
                price=price,
            )
        )
        return self

    def with_delivery(self, consumer, producer):
        delivery = Delivery(consumer=consumer,
                            producer=producer)
        self.deliveries[delivery.id] = delivery
        return self

    def with_company(self, company):
        self.company = company
        return self

    def _prepare_data(self):
        self.time_blocks = TimeBlock.period_to_blocks(
            blocks_per_hour=self.blocks_per_hour,
            hours=self.hours,
            initial_date=self.initial_date,
            unit_prices=self.unit_prices,
        )

    def _get_producers(self):
        return {delivery.producer for delivery_id, delivery in
                self.deliveries.items()}

    def _get_consumers(self):
        return {delivery.consumer for delivery_id, delivery in
                self.deliveries.items()}

    def print(self):
        print(f"PROBLEM DATA: {self.name}")
        print("-----------------------")
        print("CONSUMERS")
        print("     Consumer  Required Units    Departure date")
        for co in self._get_consumers():
            print(
                f"  {str(co.id).rjust(11)}"
                f"  {str(co.required_units).rjust(14)}"
                f"  {co.departure_date.strftime('%Y-%m-%d %H:%M').rjust(16)}"
            )
        print("")
        print("PRODUCERS")
        print("  Producer  Max Units/block")
        for pr in self._get_producers():
            print(
                f"  {str(pr.id).rjust(8)}"
                f"  {str(pr.max_supply).rjust(15)}"
            )
        print("")
        print("DELIVERIES")
        print("  Producer     Consumer")
        for delivery_id, delivery in self.deliveries.items():
            print(
                f"  {str(delivery.producer.id).rjust(8)}"
                f"  {str(delivery.consumer.id).rjust(11)}"
            )
        print("")
        print("TIME BLOCKS AND UNIT PRICES")
        print(f"           #              From             Until       Price")
        for block_number, block in self.time_blocks.items():
            date_from = "".rjust(19)
            if block.date_from:
                date_from = block.date_from.strftime('%Y-%m-%d %H:%M').rjust(16)
            date_until = "".rjust(19)
            if block.date_until:
                date_until = block.date_until.strftime('%Y-%m-%d %H:%M').rjust(
                    16)

            print(f"  {block_number.rjust(10)}"
                  f"  {date_from}"
                  f"  {date_until}"
                  f"  {str(block.price).rjust(10)}")

    def solve(self, print_results=True):
        self._prepare_data()
        # Instantiate a Glop solver, naming it SmartGridSolver.
        self.solver = pywraplp.Solver('ProducerConsumerSolver',
                                      pywraplp.Solver.GLOP_LINEAR_PROGRAMMING)
        # we'll need a variable for each combination of delivery and block
        # but only if the block ends before the consumer departure date
        self.variables = {
            delivery_id: {}
            for delivery_id, delivery in self.deliveries.items()
        }

        # Objective: minimize the sum delivery in a block * unit price.
        objective = self.solver.Objective()
        for delivery_id, delivery in self.deliveries.items():
            for block_number in range(0, len(self.time_blocks)):
                time_block = self.time_blocks.get(str(block_number))
                if time_block.date_until <= delivery.consumer.departure_date:
                    self.variables.get(delivery.id)[
                        block_number] = self.solver.NumVar(
                        0.0,
                        self.solver.infinity(),
                        f"{delivery.id}-{time_block.number}",
                    )
                    objective.SetCoefficient(
                        self.variables.get(delivery.id)[block_number],
                        time_block.price
                    )
        objective.SetMinimization()
        # constraints
        # the sum of all variables for the same delivery must be equal to the
        # amount of units the consumer requires
        consumer_constraints = {}
        for delivery_id, delivery in self.deliveries.items():
            consumer = delivery.consumer
            consumer_constraints[consumer.id] = self.solver.Constraint(
                consumer.required_units,
                consumer.required_units,
            )
            for block_number in range(0, len(self.time_blocks)):
                variable = self.variables.get(delivery.id).get(block_number)
                if variable:
                    consumer_constraints[consumer.id].SetCoefficient(variable, 1)
        # a producer has a maximum supply
        # so we have to create a constraint for each producer and
        # time block
        for pr in self._get_producers():
            for block_number in range(0, len(self.time_blocks)):
                constraint = self.solver.Constraint(
                    0,
                    pr.max_supply
                )
                for delivery_id, delivery_vars in self.variables.items():
                    delivery = self.deliveries.get(delivery_id)
                    if pr.id == delivery.producer.id:
                        variable = delivery_vars.get(block_number)
                        if variable:
                            constraint.SetCoefficient(variable, 1)
        # TODO: add building and company constraints
        # for building, the sum of all variables for the same building
        # and a specific time block should not be higher than the building
        # max supply. The same applies to company.

        # Solve!
        status = self.solver.Solve()
        schedule = Schedule(self.name, status, self)
        if print_results:
            schedule.print()
        return schedule
