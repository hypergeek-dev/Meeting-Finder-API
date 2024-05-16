using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;

class Program
{
    static async Task Main()
    {
        try
        {
            string rawJsonFile = "rawData.json";
            string processedJsonFile = "processedData.json";

            await FetchAndSaveJsonData(rawJsonFile);
            await ProcessAndSaveData(rawJsonFile, processedJsonFile);
            await InsertNewDataFromJson(processedJsonFile);

            Console.WriteLine("Data processing and insertion completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
        finally
        {
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }
    }

    static async Task FetchAndSaveJsonData(string filePath)
    {
        string jsonUrl = "https://bmlt.virtual-na.org/main_server/client_interface/jsonp/?switcher=GetSearchResults&get_used_formats&lang_enum=en&data_field_key=location_postal_code_1,duration_time,start_time,time_zone,weekday_tinyint,service_body_bigint,longitude,latitude,location_province,location_municipality,location_street,location_info,location_neighborhood,formats,format_shared_id_list,meeting_name,location_sub_province,worldid_mixed,root_server_uri,id_bigint,venue_type,location_text,virtual_meeting_additional_info,virtual_meeting_link,phone_meeting_number,contact_name_1,contact_phone_1,contact_email_1,contact_name_2,contact_phone_2,contact_email_2,wheelchair&services[]=4&recursive=1&sort_keys=start_time&callback=";

        using (var httpClient = new HttpClient())
        {
            string jsonText = await httpClient.GetStringAsync(jsonUrl);
            await File.WriteAllTextAsync(filePath, jsonText);
        }
    }

    static async Task ProcessAndSaveData(string inputFilePath, string outputFilePath)
    {
        string jsonText = await File.ReadAllTextAsync(inputFilePath);
        RootObject rootObject = JsonConvert.DeserializeObject<RootObject>(jsonText);

        List<ProcessedMeeting> processedMeetings = new List<ProcessedMeeting>();

        foreach (Meeting meeting in rootObject.Meetings)
        {
            try
            {
                string utcOffset = null;

                if (!string.IsNullOrEmpty(meeting.time_zone))
                {
                    utcOffset = await GetUtcOffsetByTimeZone(meeting.time_zone);
                }

                if (string.IsNullOrEmpty(utcOffset) && !string.IsNullOrEmpty(meeting.latitude) && !string.IsNullOrEmpty(meeting.longitude))
                {
                    string timeZone = await GetTimeZoneByCoordinates(meeting.latitude, meeting.longitude);
                    if (!string.IsNullOrEmpty(timeZone))
                    {
                        utcOffset = await GetUtcOffsetByTimeZone(timeZone);
                    }
                }

                if (string.IsNullOrEmpty(utcOffset))
                {
                    throw new Exception("Failed to fetch UTC offset. Offset data is null or incomplete.");
                }

                // Parse start_time and duration_time
                DateTime startTime = DateTime.ParseExact(meeting.start_time, "HH:mm:ss", CultureInfo.InvariantCulture);
                TimeSpan durationTime = TimeSpan.Parse(meeting.duration_time);
                decimal utcOffsetDecimal = decimal.Parse(utcOffset);

                // Calculate UTC start and end times
                DateTime utcStartTime = startTime.AddHours(-(double)utcOffsetDecimal);
                DateTime utcEndTime = utcStartTime.Add(durationTime);

                processedMeetings.Add(new ProcessedMeeting
                {
                    id_bigint = meeting.id_bigint,
                    worldid_mixed = meeting.worldid_mixed,
                    service_body_bigint = meeting.service_body_bigint,
                    weekday_tinyint = meeting.weekday_tinyint,
                    venue_type = meeting.venue_type,
                    start_time = meeting.start_time,
                    duration_time = meeting.duration_time,
                    time_zone = meeting.time_zone,
                    formats = meeting.formats,
                    longitude = meeting.longitude,
                    latitude = meeting.latitude,
                    root_server_uri = meeting.root_server_uri,
                    format_shared_id_list = meeting.format_shared_id_list,
                    meeting_name = meeting.meeting_name,
                    location_text = meeting.location_text,
                    location_info = meeting.location_info,
                    location_street = meeting.location_street,
                    location_neighborhood = meeting.location_neighborhood,
                    location_municipality = meeting.location_municipality,
                    location_sub_province = meeting.location_sub_province,
                    location_province = meeting.location_province,
                    location_postal_code_1 = meeting.location_postal_code_1,
                    contact_phone_2 = meeting.contact_phone_2,
                    contact_email_2 = meeting.contact_email_2,
                    contact_name_2 = meeting.contact_name_2,
                    contact_phone_1 = meeting.contact_phone_1,
                    contact_email_1 = meeting.contact_email_1,
                    contact_name_1 = meeting.contact_name_1,
                    phone_meeting_number = meeting.phone_meeting_number,
                    virtual_meeting_link = meeting.virtual_meeting_link,
                    virtual_meeting_additional_info = meeting.virtual_meeting_additional_info,
                    UTC_offset = utcOffsetDecimal,
                    UTC_start_time = utcStartTime,
                    UTC_end_time = utcEndTime
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing meeting: {ex.Message}");
                Console.WriteLine($"Meeting data: {JsonConvert.SerializeObject(meeting)}");
            }
        }

        string processedJsonText = JsonConvert.SerializeObject(processedMeetings);
        await File.WriteAllTextAsync(outputFilePath, processedJsonText);
    }

    static async Task InsertNewDataFromJson(string filePath)
    {
        string jsonText = await File.ReadAllTextAsync(filePath);
        List<ProcessedMeeting> processedMeetings = JsonConvert.DeserializeObject<List<ProcessedMeeting>>(jsonText);

        string connectionString = Environment.GetEnvironmentVariable("DB_CONNECTION_STRING");

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            Console.WriteLine("Connected to SQL Server!");

            // Check and create UTC_start_time and UTC_end_time columns if they don't exist
            await EnsureColumnExists(connection, "Meetings", "UTC_start_time", "DATETIME");
            await EnsureColumnExists(connection, "Meetings", "UTC_end_time", "DATETIME");

            string clearDataQuery = "DELETE FROM Meetings";
            using (SqlCommand clearCommand = new SqlCommand(clearDataQuery, connection))
            {
                await clearCommand.ExecuteNonQueryAsync();
            }

            foreach (ProcessedMeeting meeting in processedMeetings)
            {
                try
                {
                    string query = "INSERT INTO Meetings (id_bigint, worldid_mixed, service_body_bigint, " +
                                   "weekday_tinyint, venue_type, start_time, duration_time, time_zone, formats, " +
                                   "longitude, latitude, root_server_uri, format_shared_id_list, meeting_name, " +
                                   "location_text, location_info, location_street, location_neighborhood, " +
                                   "location_municipality, location_sub_province, location_province, " +
                                   "location_postal_code_1, contact_phone_2, contact_email_2, " +
                                   "contact_name_2, contact_phone_1, contact_email_1, contact_name_1, " +
                                   "phone_meeting_number, virtual_meeting_link, virtual_meeting_additional_info, " +
                                   "UTC_offset, UTC_start_time, UTC_end_time) " +
                                   "VALUES (@id_bigint, @worldid_mixed, @service_body_bigint, " +
                                   "@weekday_tinyint, @venue_type, @start_time, @duration_time, @time_zone, " +
                                   "@formats, @longitude, @latitude, @root_server_uri, @format_shared_id_list, " +
                                   "@meeting_name, @location_text, @location_info, @location_street, " +
                                   "@location_neighborhood, @location_municipality, @location_sub_province, " +
                                   "@location_province, @location_postal_code_1, @contact_phone_2, " +
                                   "@contact_email_2, @contact_name_2, @contact_phone_1, @contact_email_1, " +
                                   "@contact_name_1, @phone_meeting_number, @virtual_meeting_link, " +
                                   "@virtual_meeting_additional_info, @UTC_offset, @UTC_start_time, @UTC_end_time)";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@id_bigint", meeting.id_bigint);
                        command.Parameters.AddWithValue("@worldid_mixed", meeting.worldid_mixed);
                        command.Parameters.AddWithValue("@service_body_bigint", meeting.service_body_bigint);
                        command.Parameters.AddWithValue("@weekday_tinyint", meeting.weekday_tinyint);
                        command.Parameters.AddWithValue("@venue_type", meeting.venue_type);
                        command.Parameters.AddWithValue("@start_time", meeting.start_time);
                        command.Parameters.AddWithValue("@duration_time", meeting.duration_time);
                        command.Parameters.AddWithValue("@time_zone", meeting.time_zone);
                        command.Parameters.AddWithValue("@formats", meeting.formats);
                        command.Parameters.AddWithValue("@longitude", meeting.longitude);
                        command.Parameters.AddWithValue("@latitude", meeting.latitude);
                        command.Parameters.AddWithValue("@root_server_uri", meeting.root_server_uri);
                        command.Parameters.AddWithValue("@format_shared_id_list", meeting.format_shared_id_list);
                        command.Parameters.AddWithValue("@meeting_name", meeting.meeting_name);
                        command.Parameters.AddWithValue("@location_text", meeting.location_text);
                        command.Parameters.AddWithValue("@location_info", meeting.location_info);
                        command.Parameters.AddWithValue("@location_street", meeting.location_street);
                        command.Parameters.AddWithValue("@location_neighborhood", meeting.location_neighborhood);
                        command.Parameters.AddWithValue("@location_municipality", meeting.location_municipality);
                        command.Parameters.AddWithValue("@location_sub_province", meeting.location_sub_province);
                        command.Parameters.AddWithValue("@location_province", meeting.location_province);
                        command.Parameters.AddWithValue("@location_postal_code_1", meeting.location_postal_code_1);
                        command.Parameters.AddWithValue("@contact_phone_2", meeting.contact_phone_2);
                        command.Parameters.AddWithValue("@contact_email_2", meeting.contact_email_2);
                        command.Parameters.AddWithValue("@contact_name_2", meeting.contact_name_2);
                        command.Parameters.AddWithValue("@contact_phone_1", meeting.contact_phone_1);
                        command.Parameters.AddWithValue("@contact_email_1", meeting.contact_email_1);
                        command.Parameters.AddWithValue("@contact_name_1", meeting.contact_name_1);
                        command.Parameters.AddWithValue("@phone_meeting_number", meeting.phone_meeting_number);
                        command.Parameters.AddWithValue("@virtual_meeting_link", meeting.virtual_meeting_link);
                        command.Parameters.AddWithValue("@virtual_meeting_additional_info", meeting.virtual_meeting_additional_info);
                        command.Parameters.AddWithValue("@UTC_offset", meeting.UTC_offset);
                        command.Parameters.AddWithValue("@UTC_start_time", meeting.UTC_start_time);
                        command.Parameters.AddWithValue("@UTC_end_time", meeting.UTC_end_time);

                        await command.ExecuteNonQueryAsync();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error inserting meeting: {ex.Message}");
                    Console.WriteLine($"Meeting data: {JsonConvert.SerializeObject(meeting)}");
                }
            }
        }
    }

    static async Task EnsureColumnExists(SqlConnection connection, string tableName, string columnName, string columnType)
    {
        string checkColumnQuery = $"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}' AND COLUMN_NAME = '{columnName}') " +
                                  $"ALTER TABLE {tableName} ADD {columnName} {columnType}";
        using (SqlCommand command = new SqlCommand(checkColumnQuery, connection))
        {
            await command.ExecuteNonQueryAsync();
        }
    }

    static async Task<string> GetTimeZoneByCoordinates(string latitude, string longitude)
    {
        string apiUrl = $"https://www.timeapi.io/api/TimeZone/coordinate?latitude={latitude}&longitude={longitude}";

        using (var httpClient = new HttpClient())
        {
            HttpResponseMessage response = await RetryPolicy(async () => await httpClient.GetAsync(apiUrl));
            if (response.IsSuccessStatusCode)
            {
                string jsonResponse = await response.Content.ReadAsStringAsync();
                dynamic data = JsonConvert.DeserializeObject(jsonResponse);
                return data?.timeZone?.ToString();
            }
            else
            {
                throw new Exception($"Failed to fetch time zone by coordinates. HTTP status code: {response.StatusCode}");
            }
        }
    }

    static async Task<string> GetUtcOffsetByTimeZone(string timeZone)
    {
        string apiUrl = $"https://www.timeapi.io/api/TimeZone/zone?timeZone={timeZone}";

        using (var httpClient = new HttpClient())
        {
            HttpResponseMessage response = await RetryPolicy(async () => await httpClient.GetAsync(apiUrl));
            if (response.IsSuccessStatusCode)
            {
                string jsonResponse = await response.Content.ReadAsStringAsync();
                dynamic offsetData = JsonConvert.DeserializeObject(jsonResponse);
                int offsetSeconds = offsetData?.currentUtcOffset?.seconds ?? 0;
                if (offsetSeconds == 0 && !offsetData?.currentUtcOffset.HasValues)
                {
                    throw new Exception("Offset data is null or incomplete.");
                }
                return (offsetSeconds / 3600.0m).ToString("0.00");
            }
            else
            {
                throw new Exception($"Failed to fetch UTC offset for time zone {timeZone}. HTTP status code: {response.StatusCode}");
            }
        }
    }

    static async Task<HttpResponseMessage> RetryPolicy(Func<Task<HttpResponseMessage>> action, int retryCount = 3, int delayMilliseconds = 2000)
    {
        for (int i = 0; i < retryCount; i++)
        {
            try
            {
                return await action();
            }
            catch
            {
                if (i == retryCount - 1) throw;
                await Task.Delay(delayMilliseconds);
            }
        }
        return null;
    }

    public class Meeting
    {
        public string id_bigint { get; set; }
        public string worldid_mixed { get; set; }
        public string service_body_bigint { get; set; }
        public string weekday_tinyint { get; set; }
        public string venue_type { get; set; }
        public string start_time { get; set; }
        public string duration_time { get; set; }
        public string time_zone { get; set; }
        public string formats { get; set; }
        public string longitude { get; set; }
        public string latitude { get; set; }
        public string root_server_uri { get; set; }
        public string format_shared_id_list { get; set; }
        public string meeting_name { get; set; }
        public string location_text { get; set; }
        public string location_info { get; set; }
        public string location_street { get; set; }
        public string location_neighborhood { get; set; }
        public string location_municipality { get; set; }
        public string location_sub_province { get; set; }
        public string location_province { get; set; }
        public string location_postal_code_1 { get; set; }
        public string contact_phone_2 { get; set; }
        public string contact_email_2 { get; set; }
        public string contact_name_2 { get; set; }
        public string contact_phone_1 { get; set; }
        public string contact_email_1 { get; set; }
        public string contact_name_1 { get; set; }
        public string phone_meeting_number { get; set; }
        public string virtual_meeting_link { get; set; }
        public string virtual_meeting_additional_info { get; set; }
    }

    public class ProcessedMeeting : Meeting
    {
        public decimal UTC_offset { get; set; }
        public DateTime UTC_start_time { get; set; }
        public DateTime UTC_end_time { get; set; }
    }

    public class RootObject
    {
        public List<Meeting> Meetings { get; set; }
    }
}
