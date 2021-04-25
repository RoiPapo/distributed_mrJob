import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime


class Bus(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_rows,  # Get (Region, Country)
                   reducer=self.reducer_find_journey_time),
            MRStep(mapper=self.mapper_change_format,
                   reducer=self.reducer_statistics)
        ]

    def mapper_get_rows(self, _, row):
        rows_dict = json.loads(row)
        date_time = datetime.datetime.fromtimestamp(rows_dict["Timestamp"])
        projection = (rows_dict["JourneyId"], str(date_time.date()))
        yield projection, rows_dict["Timestamp"]

    def reducer_find_journey_time(self, projection, times):
        timeList = list(times)
        first_time = min(timeList)
        last_time = max(timeList)
        totaltime = last_time - first_time
        yield projection, int(totaltime)

    def mapper_change_format(self, projection, totaltime):
        yield projection[0], (totaltime, projection[1])

    def reducer_statistics(self, journeyID, data):
        datalist = list(data)
        ride_times = []
        date_times = []
        for value in datalist:
            ride_times.append(datalist[0][0])
            date_times.append(datalist[0])
            yield (journeyID, datalist[0][1]), datalist[0][0]
        AVG_Time = sum(ride_times) / len(ride_times)
        AVG_dif_time = []
        for date in date_times:
            AVG_dif_time.append((abs(datalist[0][0] - AVG_Time), datalist[0][1]))
        ordered_times_list = sorted(AVG_dif_time)
        yield journeyID, (AVG_Time, ordered_times_list[0][1])


def main():
    Bus.run()


if __name__ == "__main__":
    main()
