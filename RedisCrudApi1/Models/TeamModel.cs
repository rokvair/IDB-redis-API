using System.ComponentModel.DataAnnotations;

namespace Models;
public class TeamModel
{
    [Required]
    public string Country { get; set; }

    [Required]
    public string Name { get; set; }

    [Required]
    public string City { get; set; }

    [Required]
    public float Value { get; set; }

    [Required]
    public int Created_at { get; set; }
}