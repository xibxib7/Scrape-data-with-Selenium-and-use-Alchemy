from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

import psycopg2
from psycopg2.errors import UniqueViolation

from sqlalchemy import create_engine
from sqlalchemy import Table, Column
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Integer, String, BigInteger
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import CheckConstraint
from sqlalchemy import UniqueConstraint
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

base = declarative_base()

class Movie(base):
    __tablename__ = 'movie'

    movie_id = Column(Integer(), primary_key=True)
    title = Column(String(100), nullable=False)
    year = Column(Integer(), nullable=True)
    parental_guide = Column(String(20), nullable=True)
    runtime = Column(String(10), nullable=True)
    gross_us_ca_sale = Column(BigInteger(), nullable=True)

    CheckConstraint('movie_id >= 0')

class Person(base):
    __tablename__ = 'persons'

    person_id = Column(Integer(), primary_key=True)
    person_name = Column(String(70), nullable=False)

class MovieGenres(base):
    __tablename__ = 'movie_genres'

    movie_id = Column(Integer(), ForeignKey('movie.movie_id'), primary_key=True)
    genre_name = Column(String(20), primary_key=True)

class MovieDirectors(base):
    __tablename__ = 'movie_directors'

    movie_id = Column(Integer(), ForeignKey('movie.movie_id'), primary_key=True)
    person_id = Column(Integer(), ForeignKey('persons.person_id'), primary_key=True)

class MovieWriters(base):
    __tablename__ = 'movie_writers'

    movie_id = Column(Integer(), ForeignKey('movie.movie_id'), primary_key=True)
    person_id = Column(Integer(), ForeignKey('persons.person_id'), primary_key=True)

class MovieStars(base):
    __tablename__ = 'movie_stars'

    movie_id = Column(Integer(), ForeignKey('movie.movie_id'), primary_key=True)
    person_id = Column(Integer(), ForeignKey('persons.person_id'), primary_key=True)

engine = create_engine('postgresql+psycopg2://postgres:!Mohammad11271@localhost:5432/imdb_data')
base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()
session.commit()

exec_path = './chrome_driver/chromedriver.exe'
webpage_address = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'

service = Service(executable_path=exec_path)
options = Options()
options.add_argument('--lang=en-US')
options.add_argument('--disable-gpu')
#options.add_argument('--window-size=1920,1080')
driver = webdriver.Chrome(service=service, options=options)

driver.get(webpage_address)

titles = driver.find_elements(By.XPATH, value='//li[contains(@class, "ipc-metadata-list-summary-item")]')
print(len(titles))

pages = []
for title in titles:
    pages.append(title.find_element(By.XPATH, value='.//a[contains(@class, "ipc-title-link-wrapper")]').get_property('href'))



rank = []
title_list = []
year_list = []
parental_guide_list = []
runtime_list = []
genre_list = []
director_list = []
writer_list = []
star_list = []
gross_list = []

for i in range(250):

    driver.get(pages[i])

    film_id_value = int(pages[i][29:36])
    # Fetching film name
    try:
        title = driver.find_element(By.XPATH, value='//span[@data-testid="hero__primary-text"]')
        title_list.append(title.text)
    except NoSuchElementException:
        title_list.append(None)

    

    # Fetching year of production
    try:
        year = driver.find_element(By.XPATH, value = '//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/ul/li[1]/a')
        year_list.append(year.text)
    except NoSuchElementException:
        year_list.append(None)

    # Fetching parental_guide
    try:
        parental_guide = driver.find_element(By.XPATH, value= '//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/ul/li[2]/a')
        parental_guide_list.append(parental_guide.text)
    except NoSuchElementException:
        parental_guide_list.append(None)


    # Fetching duration
    try:
        runtime = driver.find_element(By.XPATH, value='//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[2]/div[1]/ul/li[3]')
        runtime_list.append(runtime.text)
    except NoSuchElementException:
        runtime_list.append(None)

    # Fetching gross_us_canada
    try:
        box_office = driver.find_element(By.XPATH, value='//li[@data-testid="title-boxoffice-grossdomestic"]')
        name_string = box_office.text.split('\n')
        gross_list.append((name_string[1][1:]).replace(',', ''))
    except NoSuchElementException:
        gross_list.append(None)

    movie_record = Movie(movie_id = film_id_value,
                       title= title_list[i],
                       year= year_list[i],
                       parental_guide = parental_guide_list[i],
                       runtime= runtime_list[i],
                       gross_us_ca_sale= gross_list[i])
    try:
        session.add(movie_record)
        session.commit()
    except IntegrityError as ie:
        session.rollback()

    
    # fetching genre names
    try:
        genres = driver.find_element(By.XPATH, value='//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[1]/section/div[1]/div[2]')
        genres = genres.find_elements(By.TAG_NAME, 'a')
        
        for genre in genres:
            movie_genre_record = MovieGenres(movie_id = film_id_value, genre_name = genre.text.strip())

            try:
                session.add(movie_genre_record)
                session.commit()
            except IntegrityError as ie:
                session.rollback()

    except NoSuchElementException:
        print('Log: ' + film_id_value + ' genres list not found')


    # fetching director name(s)
    try:
        directors = driver.find_element(By.XPATH, value='//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[2]/div[2]/div/ul/li[1]/div')
        directors = directors.find_elements(By.TAG_NAME, 'a')

        for director in directors:
            link_name = director.get_property('href')
            person_id = link_name[28:35]

            try:
                person_record = Person(person_id = int(person_id), person_name = director.text.strip())
                print(director.text.strip())
                session.add(person_record)
                session.commit()
            except IntegrityError as ie:
                session.rollback()

            try:
                movie_director_record = MovieDirectors(movie_id = film_id_value, person_id = int(person_id))
                session.add(movie_director_record)
                session.commit()
            except IntegrityError as ie:
                session.rollback()

    except NoSuchElementException:
        director_list.append(None)

    

    # Fetching writers name(s)
    try:
        writers = driver.find_element(By.XPATH, value='//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[2]/div[2]/div/ul/li[2]/div')
        writers = writers.find_elements(By.TAG_NAME, 'a')

        for writer in writers:
            link_name = writer.get_property('href')
            person_id = link_name[28:35]

            person_record = Person(person_id = int(person_id), person_name = writer.text.strip())
            movie_writer_record = MovieWriters(movie_id = film_id_value, person_id = int(person_id))

            try:
                session.add(person_record)
                session.commit()
            except IntegrityError as ie:
                session.rollback()

            try:
                session.add(movie_writer_record)
                session.commit()
            except IntegrityError as ie:
                session.rollback()


    except NoSuchElementException:
        print('Log: film_id' + film_id_value + ' ---> writers not found!')

    # Fetching star name(s)
    try:
        stars = driver.find_element(By.XPATH, value='//*[@id="__next"]/main/div/section[1]/section/div[3]/section/section/div[3]/div[2]/div[2]/div[2]/div/ul/li[3]/div')
        stars = stars.find_elements(By.TAG_NAME, 'a')

        for star in stars:
            link_name = star.get_property('href')
            person_id = link_name[28:35]

            person_record = Person(person_id = int(person_id), person_name= star.text.strip())
            movie_star_record = MovieStars(movie_id = film_id_value, person_id = int(person_id))

            try:
                session.add(person_record)
                session.commit()
            except IntegrityError as ie:
                session.rollback()

            try:
                session.add(movie_star_record)
                session.commit()
            except IntegrityError as ie:
                session.rollback()

    except NoSuchElementException:
        star_list.append(None)

session.close()
driver.quit()
