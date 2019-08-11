import src.reddit_mod_data as bot
import datetime as dt

if __name__ == '__main__':

    start_time = dt.datetime.now()
    print("starting script @{} ...".format(start_time))

    # execute task
    bot.RedditModData(db_type='dev').perform_one_scan(sub_count=1)

    end_time = dt.datetime.now()
    print("starting script @{}!".format(start_time))

    duration = end_time - start_time
    print("total execution time was {}".format(duration))
    print("done!")
