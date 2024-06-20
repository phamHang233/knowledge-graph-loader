import threading
import random
import time

import numpy as np

from job.crawlers.uni_pool_data import pool_by_id, get_pool_day_datas, get_pool_hour_data
from job.services.fee_from_strategy import uniswap_strategy_algorithm
from job.utils.sqrt_price_math import convert_price_to_tick, convert_tick_to_price


class GeneticAlgorithms:
    '''
    Class representing individual in population
    '''

    def __init__(self, pool, start_timestamp, end_timestamp, protocol='ethereum'):
        self.max_tick = 0
        self.min_tick = 0
        self.pool = pool
        self.protocol = protocol
        self.generations = 4 # Số thế hệ
        self.crossover_rate = 0.8  # Tỷ lệ giao phối
        self.mutation_rate = 0.3  # Tỷ lệ đột biến
        self.num_parents = 20
        # Tạo quần thể ban đầu
        self.pool_data = pool_by_id(pool, protocol)
        self.population_size = 100
        self.end_timestamp = end_timestamp
        self.start_timestamp = start_timestamp
        self.pool_hour_data = get_pool_hour_data(pool, start_timestamp, end_timestamp, protocol)[::-1]

    def selection(self, population, fitnesses):
        # Chọn số lượng cha mẹ cần thiết
          # Điều chỉnh số lượng cha mẹ theo nhu cầu
        positive_fitnesses, negative_fitness = [], []
        positive_population, negative_population = [], []
        # Tính tổng độ phù hợp
        for id, fit in enumerate(fitnesses):
            if fit > 0:
                positive_fitnesses.append(fit)
                positive_population.append(population[id])
            else:
                negative_fitness.append(fit)
                negative_population.append(population[id])

        total_fitness = sum(positive_fitnesses)
        # Tính xác suất chọn của mỗi cá thể
        selection_probs = [fitness / total_fitness for fitness in positive_fitnesses]

        positive_number = len(positive_fitnesses)
        if positive_number < self.num_parents:
            parents = positive_population

            abs_negative_fitness = [abs(fit) for fit in negative_fitness]
            total_negative_fitness = sum(abs_negative_fitness)
            neg_selection_probs = [1 - (fitness / total_negative_fitness) for fitness in abs_negative_fitness]
            selected_index = np.random.choice(len(negative_fitness), p=neg_selection_probs, size=self.num_parents-positive_number, replace=False)
            parents.append([negative_population[id] for id in selected_index])
        else:
            # Chọn ngẫu nhiên các cá thể dựa trên xác suất
            selected_index = np.random.choice(len(positive_fitnesses), p=selection_probs, size=self.num_parents,
                                              replace=False)
            parents = [positive_population[id] for id in selected_index]
        return parents

    # def crossover(self, parents):
    #
    #     # Chọn ngẫu nhiên hai cặp cha mẹ
    #     selected_parents = random.sample(parents, 2)
    #
    #     # Lai hai cặp cha mẹ đã chọn
    #     parent1 = selected_parents[0]
    #     parent2 = selected_parents[1]
    #
    #     # Lai tuyến tính cho mỗi gen
    #     # alpha = random.random()
    #     alpha = 0.5
    #     new_min_value = int((parent1[0] * (1 - alpha) + alpha * parent2[0]))
    #     new_max_value = int((parent1[1] * (1 - alpha) + alpha * parent2[1]))
    #
    #     return [new_min_value, new_max_value]

    def generate_offspring(self, parents):
        offspring = []

        # Lặp lại quy trình lai tạo cho đến khi tạo ra đủ số lượng cá thể con
        while len(offspring) < self.population_size -self.num_parents:
            selected_parents = random.sample(parents, 2)

            # Lai hai cặp cha mẹ đã chọn
            parent1 = selected_parents[0]
            parent2 = selected_parents[1]

            # Lai tuyến tính cho mỗi gen
            # alpha = random.random()
            alpha = 0.5
            new_min_value = int((parent1[0] * (1 - alpha) + alpha * parent2[0]))
            new_max_value = int((parent1[1] * (1 - alpha) + alpha * parent2[1]))
            # new_offspring = self.crossover(parents)
            offspring.append([new_min_value, new_max_value])
            if new_min_value > self.current_tick or new_max_value < self.current_tick:
                print("VUOT QUA CURRENT TICK")
        return offspring

    # Hàm đột biến (Bit flip mutation)
    def mutation(self, offspring):
        for chromosome in offspring:
            if random.random() < self.mutation_rate:
                maximum_price_change = min(self.current_tick - chromosome[0], chromosome[0] - self.min_tick)
                delta = int(random.uniform(-maximum_price_change, maximum_price_change) * 0.5)

                # Đột biến min price
                new_value = chromosome[0] + delta

                if new_value < self.min_tick or new_value > self.current_tick:
                    print("MUTATION ERROR!!")
                else:
                    # Giữ giá trị  nếu hợp lệ
                    chromosome[0] = new_value

            if random.random() < self.mutation_rate:
                maximun_price_change = min(chromosome[1] - self.current_tick, self.max_tick - chromosome[1])
                delta = int(random.uniform(-maximun_price_change, maximun_price_change) * 0.5)
                # Đột biến max price
                new_value = chromosome[1] + delta
                if new_value > self.max_tick or new_value < self.current_tick:
                    print("MUTATION ERROR!!")
                else:
                    # Giữ giá trị  nếu hợp lệ
                    chromosome[1] = new_value
            if chromosome[0] > chromosome[1]:
                chromosome = chromosome[::-1]

        return offspring

    def fitness_worker(self, min_tick, max_tick, population, fitnesses):
        """Hàm thực thi trong mỗi luồng."""
        data = uniswap_strategy_algorithm(
            pool_data=self.pool_data, backtest_data=self.pool_hour_data, investment_amount=1000,
            min_tick=min_tick*10, max_tick=max_tick*10)
        apr = data['apr']
        population.append([min_tick, max_tick])
        fitnesses.append(apr)

    def process(self):
        self.pool_info = get_pool_day_datas(self.pool, self.protocol, from_date=self.start_timestamp,
                                                 to_date=self.end_timestamp)
        population = []
        min_price = min([float(self.pool_info[i]['low']) for i in range(len(self.pool_info))])
        max_price = max([float(self.pool_info[i]['high']) for i in range(len(self.pool_info))])
        decimals0 = int(self.pool_data['token0']['decimals'])
        decimals1 = int(self.pool_data['token1']['decimals'])

        self.current_tick = convert_price_to_tick(float(self.pool_hour_data[-1]['close']), decimals0, decimals1) /10
        self.min_tick = convert_price_to_tick(max_price, decimals0, decimals1) / 10
        self.max_tick = convert_price_to_tick(min_price, decimals0, decimals1)/ 10
        start_time = time.time()
        for _ in range(self.population_size):
            min_range = int(random.uniform(self.min_tick, self.current_tick))
            max_range = int(random.uniform(self.current_tick+1, self.max_tick))  # Giữ max_range lớn hơn min_range
            population.append([min_range, max_range])

        data, best_chromone = None, None
        # Lặp lại qua các thế hệ
        for generation in range(self.generations):
            new_population = []
            fitnesses = []
            for idx in range(len(population)):
                self.fitness_worker(population[idx][0], population[idx][1], new_population, fitnesses)

            best_index = fitnesses.index(max(fitnesses))
            best_chromone = new_population[best_index]
            data = uniswap_strategy_algorithm(
                pool_data=self.pool_data, backtest_data=self.pool_hour_data, investment_amount=1000,
                min_tick=best_chromone[0]*10, max_tick=best_chromone[1]*10)

            print(
                f'the best apr in generation {generation}st: {data.get("apr")} - {data["timeInRange"]}')
            print(f'the best range in generation {generation}st: {best_chromone}')

            parents = self.selection(new_population, fitnesses)

            # Giao phối và tạo ra con cái
            offspring = self.generate_offspring(parents)

            # Đột biến
            offspring = self.mutation(offspring)

            # Tạo quần thể mới
            population = offspring + parents
        best_range = [convert_tick_to_price(tick*10, decimals0, decimals1) for tick in best_chromone]
        print('time toke', time.time() - start_time)
        return data, best_range
# def convert_tick_to_price(tick, decimals0, decimals1):
#     return 1 / (1.0001 ** tick / 10 ** (decimals1 - decimals0))
#
#
# def convert_price_to_tick(price, decimals0, decimals1):
#     return int(math.log(1 / price * 10 ** (decimals1 - decimals0), 1.0001))
# print(convert_price_to_tick(3590.31, 6,18))