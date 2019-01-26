from collections import defaultdict

from django.test import SimpleTestCase
from datetime import datetime, timedelta

from linear.algorithms.producer_consumer import *


class ProducerConsumerAlgorithmTestCase(SimpleTestCase):

    def _check_schedule(self, schedule,
                        total_price, block_numbers,
                        units_to_supply, prices,
                        expected_total_per_consumer=None):
        schedule_block_numbers = [delivery['time_block']
                                  for delivery in schedule.deliveries]
        schedule_units = [delivery['units_to_supply']
                          for delivery in schedule.deliveries]
        schedule_unit_prices = [delivery['unit_price']
                                for delivery in schedule.deliveries]
        self.assertEqual(schedule.total_price, total_price)
        self.assertEqual(sorted(block_numbers), sorted(schedule_block_numbers))
        self.assertEqual(units_to_supply, schedule_units)
        self.assertEqual(prices, schedule_unit_prices)
        if expected_total_per_consumer:
            total_per_co = defaultdict(int)
            for delivery in schedule.deliveries:
                total_per_co[delivery['consumer']] += delivery['units_to_supply']
            self.assertDictEqual(
                total_per_co,
                expected_total_per_consumer,
            )

    def setUp(self):
        self.initial_date = datetime(2019, 1, 2, 0, 0, 0)
        self.consumer = Consumer(
            id='co1',
            required_units=4,
            departure_date=self.initial_date + timedelta(hours=2),
        )
        self.company = Company(id="st1")
        self.building = Building(id="dv1", company=self.company)
        self.producer = Producer(id="cp1", max_supply=1, building=self.building)

    def test_one_ev_one_cp_simple(self):
        """
        A single Consumer and a single Producer
        2 hours of 15 minutes time blocks, consumer needs one hour to get units
        First hour is more expensive
        The resulting schedule should be the second hour, 1 unit per block
        """
        alg = ProducerConsumerAlgorithm("Simple one Producer and one Consumer")
        schedule = (
            alg.with_initial_date(self.initial_date)
            .with_hours(2)
            .with_blocks_per_hour(4)
            .with_company(self.company)
            .with_unit_price(
                date_from=self.initial_date,
                date_until=self.initial_date + timedelta(hours=1),
                price=2)
            .with_unit_price(
                date_from=self.initial_date + timedelta(hours=1),
                date_until=self.initial_date + timedelta(hours=2),
                price=1)
            .with_delivery(consumer=self.consumer, producer=self.producer)
            .solve(print_results=True)
        )
        self.assertTrue(schedule.solved)
        self.assertEqual(len(schedule.deliveries), 4)
        self._check_schedule(
            schedule,
            total_price=4.0,
            block_numbers=[4, 5, 6, 7],
            units_to_supply=[1.0, 1.0, 1.0, 1.0],
            prices=[1, 1, 1, 1],
        )

    def test_one_ev_one_cp_interleaved(self):
        """
        A single Consumer and a single Producer
        2 hours of 15 minutes time blocks, consumer needs one hour to get units
        Odd blocks are cheaper than the even ones
        The resulting schedule should be the odd blocks
        """

        alg = ProducerConsumerAlgorithm("Simple one Producer and one Consumer interleaved")
        schedule = (
            alg.with_initial_date(self.initial_date)
                .with_hours(2)
                .with_blocks_per_hour(4)
                .with_company(self.company)
                .with_unit_price(block_number=0, price=2)
                .with_unit_price(block_number=1, price=1)
                .with_unit_price(block_number=2, price=2)
                .with_unit_price(block_number=3, price=1)
                .with_unit_price(block_number=4, price=2)
                .with_unit_price(block_number=5, price=1)
                .with_unit_price(block_number=6, price=2)
                .with_unit_price(block_number=7, price=1)
                .with_delivery(consumer=self.consumer, producer=self.producer)
                .solve(print_results=True)
        )
        self.assertTrue(schedule.solved)
        self.assertEqual(len(schedule.deliveries), 4)
        self._check_schedule(
            schedule,
            total_price=4.0,
            block_numbers=[1, 3, 5, 7],
            units_to_supply=[1.0, 1.0, 1.0, 1.0],
            prices=[1, 1, 1, 1],
        )

    def test_one_ev_one_cp_departing_in_one_hour(self):
        """
        A single Consumer and a single Producer
        2 hours of 15 minutes time blocks, consumer needs one hour to get units
        First hour is more expensive, but consumer needs to depart after the first hour
        The resulting schedule should be the first hour
        """
        self.consumer.departure_date = self.initial_date + timedelta(hours=1)
        alg = ProducerConsumerAlgorithm("Simple one Producer and one Consumer departing in 1 hour")
        schedule = (
            alg.with_initial_date(self.initial_date)
                .with_hours(2)
                .with_blocks_per_hour(4)
                .with_company(self.company)
                .with_unit_price(
                date_from=self.initial_date,
                date_until=self.initial_date + timedelta(hours=1),
                price=2)
                .with_unit_price(
                date_from=self.initial_date + timedelta(hours=1),
                date_until=self.initial_date + timedelta(hours=2),
                price=1)
                .with_delivery(consumer=self.consumer, producer=self.producer)
                .solve(print_results=True)
        )
        self.assertTrue(schedule.solved)
        self.assertEqual(len(schedule.deliveries), 4)
        self._check_schedule(
            schedule,
            total_price=8.0,
            block_numbers=[0, 1, 2, 3],
            units_to_supply=[1.0, 1.0, 1.0, 1.0],
            prices=[2, 2, 2, 2],
        )

    def test_one_ev_one_cp_departing_one_hour_and_half(self):
        """
        A single Consumer and a single Producer
        2 hours of 15 minutes time blocks, consumer needs one hour to get units
        First hour is more expensive, but consumer needs to depart after 1h 30m
        The resulting schedule should be from 0h 30m to 1h 30m
        """
        self.consumer.departure_date = self.initial_date + timedelta(hours=1,
                                                                     minutes=30)
        alg = ProducerConsumerAlgorithm(
            "Simple one Producer and one Consumer departing in 1'5 hours")
        schedule = (
            alg.with_initial_date(self.initial_date)
                .with_hours(2)
                .with_blocks_per_hour(4)
                .with_company(self.company)
                .with_unit_price(
                date_from=self.initial_date,
                date_until=self.initial_date + timedelta(minutes=30),
                price=2)
                .with_unit_price(
                date_from=self.initial_date + timedelta(minutes=30),
                date_until=self.initial_date + timedelta(hours=1, minutes=30),
                price=1)
                .with_unit_price(
                date_from=self.initial_date + timedelta(hours=1, minutes=30),
                date_until=self.initial_date + timedelta(hours=2),
                price=2)
                .with_delivery(consumer=self.consumer, producer=self.producer)
                .solve(print_results=True)
        )
        self.assertTrue(schedule.solved)
        self.assertEqual(len(schedule.deliveries), 4)
        self._check_schedule(
            schedule,
            total_price=4.0,
            block_numbers=[2, 3, 4, 5],
            units_to_supply=[1.0, 1.0, 1.0, 1.0],
            prices=[1, 1, 1, 1],
        )

    def test_two_ev_one_cp(self):
        """
        Two Consumer and a single Producer
        2 hours of 15 minutes time blocks, first Consumer needs 1 hour,
        The second one needs 30 min
        Only a period of 30 minutes is more expensive. Algorithm should avoid it
        """
        ev2 = Consumer(
            id='ev2',
            required_units=2,
            departure_date=self.initial_date + timedelta(hours=2),
        )
        alg = ProducerConsumerAlgorithm("Simple one Producer and two Consumer")
        schedule = (
            alg.with_initial_date(self.initial_date)
                .with_hours(2)
                .with_blocks_per_hour(4)
                .with_company(self.company)
                .with_unit_price(
                date_from=self.initial_date,
                date_until=self.initial_date + timedelta(minutes=30),
                price=1)
                .with_unit_price(
                date_from=self.initial_date + timedelta(minutes=30),
                date_until=self.initial_date + timedelta(hours=1),
                price=2)
                .with_unit_price(
                date_from=self.initial_date + timedelta(hours=1),
                date_until=self.initial_date + timedelta(hours=2),
                price=1)
                .with_delivery(consumer=self.consumer, producer=self.producer)
                .with_delivery(consumer=ev2, producer=self.producer)
                .solve(print_results=True)
        )
        self.assertTrue(schedule.solved)
        self.assertEqual(len(schedule.deliveries), 6)
        self._check_schedule(
            schedule,
            total_price=6.0,
            block_numbers=[0, 1, 4, 5, 6, 7],
            units_to_supply=[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            prices=[1, 1, 1, 1, 1, 1],
            expected_total_per_consumer={'co1': 4.0, 'ev2': 2.0},
        )
