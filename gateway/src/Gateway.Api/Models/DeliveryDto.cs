namespace Gateway.Api.Models;

public class DeliveryDto {
    public string? Id { get; set; }
    public string OrderId { get; set; } = default!;
    public string DeliveryPersonId { get; set; } = default!;
    public string City { get; set; } = default!;
    public string Weather { get; set; } = default!;
    public string Traffic { get; set; } = default!;
    public double DistanceKm { get; set; }
    public double TimeTakenMin { get; set; }
    public DateTimeOffset DeliveryTimestamp { get; set; }
    public string DeliveryStatus { get; set; } = default!;
}
