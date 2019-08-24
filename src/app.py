import click
import reddit_mod_data as bot
import datetime as dt
from pbr.version import VersionInfo

info = VersionInfo(bot.__name__)
current_version = info.version_string()


@click.command()
# dev or prod
@click.option('-d', '--development', 'mode', flag_value='dev', default=True,
              help='run in dev mode')
@click.option('-p', '--production', 'mode', flag_value='prod',
              help='run in production mode')
# remote or local
@click.option('-r/-l', '--remote/--local', 'remote', is_flag=True, default=False,
              help='access a database on a remote machine using ssh')
# number of top subreddits to retrieve
@click.option('-n', '--number', 'number', type=int, default=1,
              help='number of top subreddits to retrieve')
def run_app(mode, remote, number):

    start_time = dt.datetime.now()
    click.echo("starting script @{} ...".format(start_time))
    click.echo("mode: {}".format(mode))
    click.echo("remote: {}".format(remote))
    click.echo("number: {}".format(number))

    # execute task
    bot.RedditModData(mode=mode, remote=remote).perform_one_scan(sub_count=number)

    end_time = dt.datetime.now()
    click.echo("finishing script @{} ...".format(start_time))

    duration = end_time - start_time
    click.echo("total execution time was {}".format(duration))
    click.echo("done!")


if __name__ == '__main__':
    print("running version {version}".format(version=current_version))
    run_app()


