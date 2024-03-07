from click.testing import CliRunner

from parser.cli import prospective

if __name__ == '__main__':
    runner = CliRunner()

    runner.invoke(prospective, ['-i', 'data/WEP.json', '-o', 'ro-crate-metadata.json'])
