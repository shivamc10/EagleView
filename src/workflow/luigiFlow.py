import luigi
import csv


class SplitFiles(luigi.Task):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.srcDataFile='/Users/shivamchoubey/Desktop/Personal/Test/EagleView/data/the-movies-dataset/movies_metadata.csv'
        self.outputDest='/Users/shivamchoubey/Desktop/Personal/Test/EagleView/result/{}.csv'
        self.genres=['Comedy','Romance','Others']

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('./output/splitFilesAvg.txt')


    def run(self):

        with open(self.srcDataFile,'r', encoding="utf-8") as csvfile:
            csvreader = csv.reader(csvfile)
            fields = csvreader.__next__()
            outputFile=self.outputDest.format(self.genres[0])
            comedyFile=open(outputFile,'w', encoding="utf-8")
            comedyCsv = csv.writer(comedyFile)
            comedyCsv.writerow(fields)

            outputFile=self.outputDest.format(self.genres[1])
            romanceFile=open(outputFile,'w', encoding="utf-8")
            romanceCsv = csv.writer(romanceFile)
            romanceCsv.writerow(fields)

            outputFile=self.outputDest.format(self.genres[2])
            otherFile=open(outputFile,'w', encoding="utf-8")
            otherCsv = csv.writer(otherFile)
            otherCsv.writerow(fields)

            for row in csvreader:
                gList=row[3]
                other = 1
                if 'Comedy' in gList:
                    comedyCsv.writerow(row)
                    other=0
                if 'Romance' in gList:
                    romanceCsv.writerow(row)
                    other=0
                if other==1:
                    otherCsv.writerow(row)
            comedyFile.close()
            romanceFile.close()
            otherFile.close()
        yield Average(genres=self.genres, outputDest=self.outputDest)
        yield FinalOutput()



class Average(luigi.WrapperTask):
    genres=luigi.parameter.ListParameter()
    outputDest=luigi.parameter.Parameter()

    def requires(self):
        for genre in self.genres:
            if genre=='Comedy':
                yield ComedyAvg(srcDataFile=self.outputDest.format(self.genres[0]))
            if genre=='Romance':
                yield RomanceAvg(srcDataFile=self.outputDest.format(self.genres[1]))
            else:
                yield OtherAvg(srcDataFile=self.outputDest.format(self.genres[2]))


class ComedyAvg(luigi.ExternalTask):
    srcDataFile=luigi.parameter.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('./output/comedyAvg.txt')

    def run(self):
        revenue=0
        rowCount=0
        with open(self.srcDataFile,'r', encoding="utf-8") as csvfile:
            csvreader = csv.reader(csvfile)
            fields = csvreader.__next__()
            for row in csvreader:
                try:
                    revenue=revenue+float(row[15])
                    rowCount=rowCount+1
                except:
                    rowCount=rowCount+1
        print(revenue/rowCount)
        with self.output().open('w') as out_file:
            out_file.write('Avg Popularity\n')
            out_file.write(str(revenue/rowCount))


class RomanceAvg(luigi.ExternalTask):
    srcDataFile=luigi.parameter.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('./output/romanceAvg.txt')

    def run(self):
        runTime=0
        rowCount=0
        with open(self.srcDataFile,'r', encoding="utf-8") as csvfile:
            csvreader = csv.reader(csvfile)
            fields = csvreader.__next__()
            for row in csvreader:
                try:
                    runTime=runTime+float(row[16])
                    rowCount=rowCount+1
                except:
                    rowCount=rowCount+1
        print(runTime/rowCount)
        with self.output().open('w') as out_file:
            out_file.write('Avg Popularity\n')
            out_file.write(str(runTime/rowCount))

class OtherAvg(luigi.ExternalTask):
    srcDataFile=luigi.parameter.Parameter()

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('./output/otherAvg.txt')

    def run(self):
        popularity=0
        rowCount=0
        with open(self.srcDataFile,'r', encoding="utf-8") as csvfile:
            csvreader = csv.reader(csvfile)
            fields = csvreader.__next__()
            print(len(fields))
            for row in csvreader:
                try:
                    popularity=popularity+float(row[10])
                    rowCount=rowCount+1
                except:
                    rowCount=rowCount+1
        print(popularity/rowCount)
        with self.output().open('w') as out_file:
            out_file.write('Avg Popularity\n')
            out_file.write(str(popularity/rowCount))

class FinalOutput(luigi.ExternalTask):

    def requires(self):
        with open('./output/otherAvg.txt','r') as outAvg:
            str = outAvg.readline()
            self.avgPopularity=outAvg.readline()
        with open('./output/romanceAvg.txt','r') as romAvg:
            str = romAvg.readline()
            self.avgRuntime=romAvg.readline()
        with open('./output/comedyAvg.txt','r') as comAvg:
            str = comAvg.readline()
            self.avgRevenue=comAvg.readline()

    def output(self):
        return luigi.LocalTarget('./output/finalOutput.csv')

    def run(self):
        file = '/Users/shivamchoubey/Desktop/Personal/Test/EagleView/result/finalOutput.csv'
        fields="Avg Comedy Revenue,Avg Romane Runtime,Avg Others Popularity\n"
        with self.output().open('w') as csvFile:
            csvFile.write(fields)
            row = "{},{},{}".format(self.avgRevenue,self.avgRuntime,self.avgPopularity)
            csvFile.write(row)


if __name__ == '__main__':
    luigi.build([SplitFiles()], workers=3, local_scheduler=True)