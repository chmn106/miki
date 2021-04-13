from setuptools import find_packages, setup

setup(
	name = "miki",
	version = "2.0.0",
	packages = find_packages(exclude=("example",)),
	zip_safe = False,

	description = "A Quantitative-research Platform",
	author = "chen",
	author_email = "435469015@qq.com",

	license = "MIT License",
	platforms = "Independant",
	install_requires=[
		'numpy',
		'pandas',
		'pyecharts',
		'redis',
		'bcolz',
		'jqdatasdk',
		],
	)




