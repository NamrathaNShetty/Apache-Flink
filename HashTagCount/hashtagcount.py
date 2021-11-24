import os
import random
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes)
from pyflink.table.expressions import lit

# creating config
settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(settings)


def hashtag_count():
        try:
                input_file = '/home/namratha/Desktop/DataEngineer/Flink/HashTagCount/Input.txt'
                output_file = '/home/namratha/Desktop/DataEngineer/Flink/HashTagCount/Output'  
                # remove the output file, if there is one there already
                if os.path.isfile(output_file):
                        os.remove(output_file)
        
                # likewise, generate the input file given some parameters.
                hashtags = ['#apache', '#flink', '#python', '#aws']
                num_tweets = 1000
                with open(input_file, 'w') as f:
                        for tweet in range(num_tweets):
                                f.write('%s\n' % (random.choice(hashtags)))

                # write all the data to one file
                t_env.get_config().get_configuration().set_string("parallelism.default", "1")

                # Creating source
                t_env.create_temporary_table(
                        'source',
                TableDescriptor.for_connector('filesystem')
                        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                   .build())
                           .option('path', input_file)
                 .format('csv')
                        .build())
                tab = t_env.from_path('source')

                # Creating sink
                t_env.create_temporary_table(
                        'sink',
                TableDescriptor.for_connector('filesystem')
                        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                   .column('count', DataTypes.BIGINT())
                 .build())
                          .option('path', output_file)
                        .format('csv')          
                        .build())
                        
                tab = t_env.from_path('source')
                tab.group_by(tab.word) \
                .select(tab.word, lit(1).count) \
                .execute_insert('sink').wait()
        
        except Exception as e:
                print(e)

hashtag_count()