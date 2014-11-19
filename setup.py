import setuptools

setuptools.setup(
    name="DondeCSC",
    version="0.1.0",
    url="https://github.com/liat-donde/DondeCSC",

    author="Yossi Mamo",
    author_email="yossi@dondefashion.com",

    description="Donde Crawler/Scraper/Classifier",
    long_description=open('README.rst').read(),

    packages=setuptools.find_packages(),

    install_requires=[],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 2.7',
    ],
)
