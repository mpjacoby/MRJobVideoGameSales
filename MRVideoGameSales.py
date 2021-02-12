# -*- coding: utf-8 -*-
"""
Created on Sun Feb  7 13:55:47 2021

@author: matth
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRVideoGameSales(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_shooter_genre,
                   reducer=self.reducer_get_game_info),
            MRStep(mapper=self.mapper_make_sales_key,
                   reducer=self.reducer_output)
        ]
    
    def mapper_shooter_genre(self, _, line):
        (Name, Platform, Year, Genre, Publisher, NA_Sales, EU_Sales, JP_Sales, 
         Other_Sales, Global_Sales) = line.split(',')
        if (Genre == 'Shooter'):
            yield Name, (Publisher, float(Global_Sales))
            
    def reducer_get_game_info(self, Name, Info):
        Publisher = []
        Global_Sales = []
        for i,j in Info:
            Publisher.append(i)
            Global_Sales.append(j)
        yield Name, (Publisher, sum(Global_Sales))
        
    def mapper_make_sales_key(self, Name, PublisherSales):
       yield '%06.03f'%float(PublisherSales[1]), (Name, PublisherSales[0])
         
    def reducer_output(self, Total, Games):
        for Game in Games:
            yield Total, Game
       
if __name__ == '__main__':
    MRVideoGameSales.run()
    
# !python MRVideoGameSales.py vgsales.csv > vidya.txt