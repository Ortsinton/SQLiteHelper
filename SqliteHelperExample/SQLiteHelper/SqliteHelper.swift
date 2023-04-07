//
//  SqliteHelper.swift
//
//  Created by Jorge Sirvent on 10/11/22.
//

import Foundation
import SQLite3

public enum SQLiteError: Error {
    case OpenDatabase(_ message: String)
    case Prepare(_ message: String)
    case Step(_ message: String)
    case Bind(_ message: String)
    case Exec(_ message: String)
    case CloseCursor(_ message: String)
}

protocol UpgradeDatabaseDelegate {
    func onUpgrade(version: Int) throws
}

actor SqliteHelper {
    public enum SQLOrder: String {
        case ASC = "ASC"
        case DESC = "DESC"
    }
    
    private var db: OpaquePointer? { try? getDB() }
    private let name: String
    private let version: Int
    private var _db: OpaquePointer?
    private let onUpgrade: ((Int, isolated SqliteHelper) throws -> ())?
    
    private let CREATE_VERSION_SCHEMA_TABLE = """
    CREATE TABLE schema_versions (
     id INT PRIMARY KEY NOT NULL,
     date TEXT NOT NULL
    );
    """
    
    /*
     * name: The name of the db.
     * version: Current version of the db. If current version is newer than the one in the device it will trigger an onUpgrade.
     * onUpgrade: Block that will execute when a version upgrade is neede
     */
    init(name: String, version: Int, onUpgrade: @escaping ((Int, isolated SqliteHelper) throws -> ())) {
        self.name = name
        self.version = version
        self.onUpgrade = onUpgrade
    }
    
    private func getDB () throws -> OpaquePointer {
        if _db != nil {
            return _db!
        }
        
        let dbName = (FileUtils.applicationSupportDirectory?.appendingPathComponent(name))!
        var newDB = false
        if !FileManager.default.fileExists(atPath: dbName.path) {
            newDB = true
        }
        
        let errc = sqlite3_open_v2(dbName.path, &_db, SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil)
        guard errc == SQLITE_OK else {
            throw SQLiteError.OpenDatabase("Cannot open database at \(dbName.path): \(SqliteHelper.errorCode(errc))")
        }
        
        if newDB {
            try execSQL(CREATE_VERSION_SCHEMA_TABLE)
        }
        
        let oldVersion = (try? getVersion()) ?? Int.min
        if oldVersion < version {
            if onUpgrade != nil {
                try onUpgrade!(version, self)
            }
            try setSchema(version: version)
        }
        
        if let db = _db { return db }
        throw SQLiteError.OpenDatabase("Database did not open")
    }

    // MARK: schema migrations
    private func getVersion() throws -> Int {
        let schema_check = """
        SELECT MAX(id) FROM schema_versions;
        """
        var vqst: OpaquePointer? = nil
        try prepareStatement(db!, query: schema_check, -1, &vqst)
        let errc = sqlite3_step(vqst)
        guard errc == SQLITE_ROW else {
            throw SQLiteError.Step("checking version: \(SqliteHelper.errorCode(errc))")
        }
        
        let version = Int(sqlite3_column_int(vqst, 0))
        sqlite3_finalize(vqst)
        return version
    }

    private func setSchema(version: Int) throws {
        let insert_version = """
        INSERT INTO schema_versions (id, date) VALUES (?, ?);
        """
        
        var ist: OpaquePointer? = nil
        try prepareStatement(db!, query: insert_version, -1, &ist)
        
        sqlite3_bind_int(ist, 1, Int32(version))
        sqlite3_bind_text(ist, 2, DateFormatter().string(from: Date()), -1, nil)
        
        let errc = sqlite3_step(ist)
        sqlite3_finalize(ist)
        guard errc == SQLITE_DONE else {
            throw SQLiteError.Step("writing migration record for version \(version): \(SqliteHelper.errorCode(errc))")
        }
    }

    // MARK: API
    /*
     * Gets a string representation of the error code.
     *
     * errc: The error code
     */
    public static func errorCode(_ errc: Int32) -> String {
        let cstr = sqlite3_errstr(errc)
        return String(cString: cstr!)
    }
    
    /*
     * Gets the db handler.
     */
    public func rawDBHandle<T>(
        _ closure: @escaping (isolated SqliteHelper, OpaquePointer) throws -> T
    ) throws -> T {
        return try closure(self, try getDB())
    }
    
    /*
     * Executes a series of statements. Statements value cannot be binded from here.
     *
     * statements: An array of sql statements.
     */
    public func executeMultiStatements(_ statements: [String]) throws {
        for stmt in statements {
            var ctst: OpaquePointer? = nil
            try prepareStatement(db!, query: stmt, -1, &ctst)
            
            let errc = sqlite3_step(ctst)
            sqlite3_finalize(ctst)
            guard errc == SQLITE_DONE else {
                throw SQLiteError.Step("executing multi statement: \(SqliteHelper.errorCode(errc))")
            }
        }
    }
    
    /*
     * Executes a single sql statement
     
     * sql: A String containing the statement in SQL format
     */
    public func execSQL(_ sql: String) throws {
        var errc: Int32 = 0
        errc = sqlite3_exec(db, sql, nil, nil, nil)
        if (errc != SQLITE_OK) {
            throw SQLiteError.Exec("Query failed because \(errc) Query: \(sql)")
        }
    }
    
    /*
     * Executes a customizable sql query.
     *
     * tableName: the name of the table to query from.
     * columns: List of columnNames that have to be retrieved.
     * selection: The where clause in template mode.
     * selectionArgs: The arguments that have to replace the ? symbols in the previous template. selectionArgs.size must be equal to the amount of '?' symbols in selection.
     * groupBy: The groupBy clause.
     * having: The having clause.
     * order: The order by clause.
     */
    public func query(_ tableName: String, _ columns: [String], _ selection: String?, _ selectionArgs: [String]?,_ groupBy: String?,_  having: String?,_ orderBy: String?,_ order: SQLOrder?) throws -> [[Any?]] {
        let columnsParameter = columns.joined(separator: ",")
        let whereParameter = selectionArgs != nil ? "WHERE \(selection!.replaceOcurrences(of: "?", withArray: selectionArgs!)) " : ""
        let groupByParameter = groupBy != nil ? "GROUP BY \(groupBy!) " : ""
        let havingParameter = having != nil ? "HAVING \(having!) " : ""
        let orderByParameter = orderBy != nil ? "ORDER BY \(orderBy!) \(order!.rawValue) " : ""
        
        let query = """
        SELECT \(columnsParameter)
        FROM \(tableName)
        \(whereParameter)
        \(groupByParameter)
        \(havingParameter)
        \(orderByParameter)
        """
        
        return try rawQuery(query)
    }
    
    /*
     * Helper class to access data after a query
     */
    public class Cursor {
        private var errc: Int32 = 0
        private var stmt: OpaquePointer
        private var closed = false
        
        init(_ stmt: OpaquePointer) {
            self.stmt = stmt
        }
        
        func moveToNext() -> Bool {
            errc = sqlite3_step(stmt)
            return errc == SQLITE_ROW
        }
        
        func close() throws {
            if (closed) { return }

            guard errc == SQLITE_DONE else {
                sqlite3_finalize(stmt)
                
                if (errc == 0) {
                    throw SQLiteError.CloseCursor("cursor.moveToNext was never called, cursor values unused")
                } else {
                    throw SQLiteError.Step("Failed \(SqliteHelper.errorCode(errc))")
                }
            }
            
            sqlite3_finalize(stmt)
            closed = true
        }
        
        func getInt(_ index: Int32) -> Int {
            Int(sqlite3_column_int(stmt, index))
        }
        
        func getIntOrNull(_ index: Int32) -> Int? {
            if (sqlite3_column_type(stmt, index) == SQLITE_NULL) {
                return nil
            }
            return Int(sqlite3_column_int(stmt, index))
        }
        
        func getString(_ index: Int32) -> String {
            String(cString: sqlite3_column_text(stmt, index))
        }
        
        func getStringOrNull(_ index: Int32) -> String? {
            if (sqlite3_column_type(stmt, index) == SQLITE_NULL) {
                return nil
            }
            return String(cString: sqlite3_column_text(stmt, index))
        }
        
        func unpack<T: Decodable>(_ index: Int32) -> T? {
            if (sqlite3_column_type(stmt, index) == SQLITE_NULL) {
                return nil
            }
            
            let data = Data(
                bytes: sqlite3_column_blob(stmt, index),
                count: Int(sqlite3_column_bytes(stmt, index))
            )
            
            return try? JSONDecoder().decode(T.self, from: data)
        }
        
        func getDataOrNull(_ index: Int32) -> Data? {
            if (sqlite3_column_blob(stmt, index) == nil) {
                return nil
            }
            return Data(bytes: sqlite3_column_blob(stmt, index), count: Int(sqlite3_column_bytes(stmt, index)))
        }
    }
    
    /*
     * Same class as query() but with diferent parameters.
     *
     * query: Query template in sql format.
     * arguments: list of arguments that replace the ? symbols in previous template.
     */
    public func rawQuery(
        _ query: String,
        _ arguments: [Any?]? = nil,
        _ closure: @escaping (Cursor) -> Void
    ) throws {
        var stmt: OpaquePointer? = nil
        try prepareStatement(db!, query: query, -1, &stmt)
        if let args = arguments {
            for (index, arg) in args.enumerated() {
                bindValue(stmt, Int32(index + 1), arg)
            }
        }
        
        
        let cursor = Cursor(stmt!)
        closure(cursor)
        try cursor.close()
    }
    
    /*
     * Another approach to a query but without templating.
     *
     * query: SQL query.
     */
    public func rawQuery(_ query: String) throws -> [[Any?]] {
        var stmt: OpaquePointer? = nil
        var ret = [[Any?]]()
        try prepareStatement(db!, query: query, -1, &stmt)
        var errc = sqlite3_step(stmt)
        
        while errc == SQLITE_ROW {
            var row = [Any?]()
            for i in 0..<sqlite3_column_count(stmt) {
                row.append(getValueColumn(stmt, of: i))
            }
            ret.append(row)
            errc = sqlite3_step(stmt)
        }
        sqlite3_finalize(stmt)
        
        guard errc == SQLITE_DONE else {
            throw SQLiteError.Step("Getting entity info: \(SqliteHelper.errorCode(errc))")
        }
        
        return ret
    }
    
    /*
     * Executes a customizable insert statement of a single row with the values in a dictionary.
     *
     * tableName: The name of the affected table.
     * values: A dictionary with the format columnName:value.
     * replaceExisting: Replace existing clause.
     * nullColumnHack: If it has a value all null values will be replace by this.
     */
    public func insert(_ tableName: String, values: [String:Any?], replaceExisting: Bool, nullColumnHack: String?) throws {
        let keys: [String] = [String](values.keys)
        let separatedValues: [Any?] = keys.map { key in
            values[key] ?? nil
        }
        
        return try insert(tableName, columnNames: keys, values: separatedValues, replaceExisting: replaceExisting, nullColumnHack: nullColumnHack)
    }
    
    /*
     * Executes a customizable insert statement of a single row.
     *
     * tableName: The name of the affected table.
     * columnNames: List of the columnNames the statement is inserting into.
     * values: List of values. values.size must be equal to columnNames.size.
     * replaceExisting: Replace existing clause.
     * nullColumnHack: If it has a value all null values will be replace by this.
     */
    public func insert(_ tableName: String, columnNames: [String], values:[Any?], replaceExisting: Bool, nullColumnHack: String? = nil) throws {
        let replaceExisting = replaceExisting ? "OR REPLACE " : ""
        let columns = columnNames.joined(separator: ", ")
        let valuesTemplate = values.map { value in
            return "?"
        }.joined(separator: ", ")
        
        let single_insert = """
        INSERT \(replaceExisting)INTO \(tableName) (\(columns)) VALUES (\(valuesTemplate);
        """
        
        var stmt: OpaquePointer? = nil
        try prepareStatement(db!, query: single_insert, -1, &stmt)
        
        for i in 1...values.count {
            bindValue(stmt, Int32(i), values[i-1])
        }

        let errc = sqlite3_step(stmt)
        sqlite3_finalize(stmt)
        guard errc == SQLITE_DONE else {
            throw SQLiteError.Step("Writing entity version \(version): \(SqliteHelper.errorCode(errc))")
        }
    }
    
    /*
     * Executes a customizable insert statement for several rows. This is used for efficiency reasons when inserting several values
     * because the statement is only prepared once.
     *
     * tableName: The name of the affected table.
     * columnNames: List of the columnNames the statement is inserting into.
     * values: Array of value rows. each row size must be equal to columnNames.size.
     * replaceExisting: Replace existing clause.
     * nullColumnHack: If it has a value all null values will be replace by this.
     */
    public func batchInsert(_ tableName: String, columnNames: [String], values: [[Any?]], replaceExisting: Bool, nullColumnHack: String? = nil) throws {
        let replaceExisting = replaceExisting ? "OR REPLACE " : ""
        let columns = columnNames.joined(separator: ", ")
        let valuesTemplate = columnNames.map { value in
            return "?"
        }.joined(separator: ", ")
        
        let multi_insert = """
        INSERT \(replaceExisting)INTO \(tableName) (\(columns)) VALUES (\(valuesTemplate));
        """
        
        var stmt: OpaquePointer? = nil
        var errc: Int32 = 0
        try prepareStatement(db!, query: multi_insert, -1, &stmt)
        
        try execSQL("BEGIN TRANSACTION;")
        
        for valueSet in values {
            for i in 1...valueSet.count {
                bindValue(stmt, Int32(i), valueSet[i-1])
            }
            
            errc = sqlite3_step(stmt)
            guard errc == SQLITE_DONE else {
                throw SQLiteError.Step("inserting value \(valueSet): \(SqliteHelper.errorCode(errc))")
            }
            
            sqlite3_clear_bindings(stmt)
            sqlite3_reset(stmt)
        }
        try execSQL("COMMIT;")
        
        sqlite3_finalize(stmt)
    }
    
    /*
     * Executes a delete statement 
     */
    public func delete(_ tableName: String, whereClause: String?, whereArgs: [String]?) throws {
        let whereParameter = whereClause != nil && !(whereClause!.isEmpty) ?
                             "WHERE \(whereClause!.replaceOcurrences(of: "?", withArray: whereArgs!)) " : ""
        let query = """
        DELETE FROM \(tableName)
        \(whereParameter)
        """
        
        var stmt: OpaquePointer? = nil
        try prepareStatement(db!, query: query, -1, &stmt)
        if !(whereArgs!.isEmpty) {
            for i in 1...whereArgs!.count {
                bindValue(stmt, Int32(i), whereArgs![i-1])
            }
        }

        let errc = sqlite3_step(stmt)
        sqlite3_finalize(stmt)
        guard errc == SQLITE_DONE else {
            throw SQLiteError.Step("Writing entity version \(version): \(SqliteHelper.errorCode(errc))")
        }
    }
    
    private func prepareStatement(_ db: OpaquePointer, query: String, _ zsql: Int32, _ statementHandle: UnsafeMutablePointer<OpaquePointer?>?) throws {
        let errc = sqlite3_prepare_v2(db, query, zsql, statementHandle, nil)
        guard errc == SQLITE_OK else {
            throw SQLiteError.Prepare("Preparing statement \(query): \(SqliteHelper.errorCode(errc))")
        }
    }
    
    // MARK: Binders and getters
    // WARNING: sqlite3 handles arguments on a 1...n mapping in sqlite3_bind_type().
    // Transform accordingly when using these functions
    
    @discardableResult
    public func bindValue(_ stmt: OpaquePointer?,_ index: Int32,_ value: Any?) -> Int32 {
        if let value = value {
            if let valueAsString = value as? String {
                return bindString(stmt, index, valueAsString)
            }
            else if let valueAsInt = value as? Int {
                return bindInt(stmt, index, valueAsInt)
            }
            else if let valueAsData = value as? Data {
                return bindData(stmt, index, valueAsData)
            }
        }
        
        return sqlite3_bind_null(stmt, index)
    }
    
    @discardableResult
    func bindInt (_ stmt: OpaquePointer?,_ index: Int32,_ value: Int?) -> Int32 {
        if let value = value {
            return sqlite3_bind_int(stmt, index, Int32(value))
        }
        return sqlite3_bind_null(stmt, index)
    }

    @discardableResult
    func bindString (_ stmt: OpaquePointer?,_ index: Int32,_ value: String?) -> Int32 {
        if let value = value {
            return sqlite3_bind_text(stmt, index, value, -1, unsafeBitCast(OpaquePointer(bitPattern: -1), to: sqlite3_destructor_type.self))
        }
        return sqlite3_bind_null(stmt, index)
    }
    
    @discardableResult
    func bindData (_ stmt: OpaquePointer?,_ index: Int32,_ value: Data?) -> Int32 {
        if let value = value {
            return sqlite3_bind_blob(stmt, index, [UInt8](value), Int32(value.count), unsafeBitCast(-1, to: sqlite3_destructor_type.self))
        }
        return sqlite3_bind_null(stmt, index)
    }
    
    public func getValueColumn(_ stmt: OpaquePointer?, of index: Int32) -> Any? {
        let type = sqlite3_column_type(stmt, index)
        
        switch type {
            case SQLITE_INTEGER:
                return Int(sqlite3_column_int(stmt, index))
            case SQLITE_TEXT:
                return String(cString: sqlite3_column_text(stmt, index))
            default:
                return nil
        }
    }
}

extension String {
    func replaceOcurrences(of target: String, withArray arrayOfReplacements: [String]) -> String {
        var ret = self
        arrayOfReplacements.forEach {
            if let range = ret.range(of: target) {
                ret.replaceSubrange(range, with: $0)
            }
        }
        return ret
    }
}

class FileUtils: NSObject {
    static var cacheDirectory:URL? {
        get {
            do {
                return try FileManager.default.url(for: .cachesDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
            }
            catch {
                print("Caches directory couldn't be retrieved.")
                return nil
            }
        }
    }
    
    static var applicationSupportDirectory:URL? {
        get {
            do {
                return try FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
            }
            catch {
                print("Application support directory couldn't be retrieved.")
                return nil
            }
        }
    }
}
