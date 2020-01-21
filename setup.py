from setuptools import setup

setup(
    name='s3backup',
    version='0.1',
    package_dir={'': 'src'},
    py_modules=['SsbConfig1'],
    url='',
    license='MIT',
    author='Piotr Broda',
    author_email='oharon@gmail.com',
    description='Synchronizes local filesystem to S3 bucket/s',

    entry_points={
        'console_scripts': ['ssbc=s3backup.cli:main']
    }
)
