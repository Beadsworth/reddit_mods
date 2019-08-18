import reddit_mod_data as bot
import datetime as dt

if __name__ == '__main__':

    mode = 'dev'

    start_time = dt.datetime.now()
    print("starting script @{} ...".format(start_time))

    # execute task
    bot.RedditModData(mode='dev', remote=False).perform_one_scan(sub_count=1000)

    end_time = dt.datetime.now()
    print("finishing script @{} ...".format(start_time))

    duration = end_time - start_time
    print("total execution time was {}".format(duration))
    print("done!")
