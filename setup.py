from setuptools import setup

setup(
    name='ratpiz',
    version='0.0.1',
    description='Simple scheduling',
    url='https://github.com/tobes/ratpiz',
    author='Toby Dacre',
    author_email='toby.junk@gmail.com',
    license='GPLv2',
    packages=['ratpiz'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'ratpiz = ratpiz.process_runner:main',
            'ratpiz-register = ratpiz.process_runner:register',
        ]
    },
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Topic :: System :: Scheduling',
    ],
)
