package main

import (
	"github.com/jcelliott/lumber"

	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const Version = "1.0.1"

type Logger interface {
	Fatal(string, ...interface{})
	Error(string, ...interface{})
	Warning(string, ...interface{})
	Info(string, ...interface{})
	Debug(string, ...interface{})
	Trace(string, ...interface{})
}

type Driver struct {
	mutex   sync.Mutex
	mutexes map[string]*sync.Mutex
	dir     string
	log     Logger
}

type Options struct {
	Logger
}

type Address struct {
	City, State, Country string
	Pincode              json.Number
}

type User struct {
	Name, Contact, Company string
	Age                    json.Number
	Address                Address
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}
	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir: dir, mutexes: make(map[string]*sync.Mutex), log: opts.Logger,
	}

	if _, err := os.Stat(dir); err != nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating the database at '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, 8755)

}

func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")

	}
	if resource == "" {
		return fmt.Errorf("Missing resouce - unable to save record(no name)")

	}

	mutex := d.GetOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	b = append(b, byte('\n'))
	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection - no place to read record")
	}

	if resource == "" {
		return fmt.Errorf("missing resource - unable to save records")
	}

	record := filepath.Join(d.dir, collection, resource)
	if _, err := Stat(record); err != nil {
		return err
	}

	b, err := os.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)

}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("missing collection - unable to read")
	}
	dir := filepath.Join(d.dir, collection)
	if _, err := Stat(dir); err != nil {
		return nil, err
	}
	files, _ := os.ReadDir(dir)

	var records []string
	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil

}

func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)
	mutex := d.GetOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := Stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file or directory named %v\n", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}
	return nil

}

func (d *Driver) GetOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func Stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

func main() {
	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("error: ", err)
	}

	employees := []User{
		{
			Name:    "Bruce",
			Contact: "9090",
			Company: "InTech",
			Age:     "50",
			Address: Address{
				City:    "Manzini",
				State:   "Manzini",
				Country: "Eswatini",
				Pincode: "H116",
			},
		},
		{
			Name:    "Bruce2",
			Contact: "9090",
			Company: "InTech",
			Age:     "50",
			Address: Address{
				City:    "Manzini",
				State:   "Manzini",
				Country: "Eswatini",
				Pincode: "H116",
			},
		},
		{
			Name:    "Bruce3",
			Contact: "9090",
			Company: "InTech",
			Age:     "50",
			Address: Address{
				City:    "Manzini",
				State:   "Manzini",
				Country: "Eswatini",
				Pincode: "H116",
			},
		},
		{
			Name:    "Bruce4",
			Contact: "9090",
			Company: "InTech",
			Age:     "50",
			Address: Address{
				City:    "Manzini",
				State:   "Manzini",
				Country: "Eswatini",
				Pincode: "H116",
			},
		},
		{
			Name:    "Bruce5",
			Contact: "9090",
			Company: "InTech",
			Age:     "50",
			Address: Address{
				City:    "Manzini",
				State:   "Manzini",
				Country: "Eswatini",
				Pincode: "H116",
			},
		},
	}

	for _, value := range employees {
		db.Write("users", value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Company: value.Company,
			Contact: value.Contact,
			Address: value.Address,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("error: ", err)
	}

	fmt.Println(records)

	allUsers := []User{}

	for _, f := range records {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println("error: ", err)

		}
		allUsers = append(allUsers, employeeFound)
	}

	fmt.Println((allUsers))

	// if err := db.Delete("user", "Bruce"); err != nil {
	// 	fmt.Println("error: ", err)
	// }

	// if err := db.Delete("user", ""); err != nil {
	// 	fmt.Println("error: ", err)
	// }
}
